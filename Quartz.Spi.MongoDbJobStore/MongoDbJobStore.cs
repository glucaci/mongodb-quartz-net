using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Impl.AdoJobStore;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;
using Quartz.Spi.MongoDbJobStore.Repositories;
using Quartz.Util;
using Calendar = Quartz.Spi.MongoDbJobStore.Models.Calendar;

namespace Quartz.Spi.MongoDbJobStore
{
    public class MongoDbJobStore : IJobStore
    {
        private static readonly DateTimeOffset? SchedulingSignalDateTime = new DateTimeOffset(1982, 6, 28, 0, 0, 0,
            TimeSpan.FromSeconds(0));

        private const string KeySignalChangeForTxCompletion = "sigChangeForTxCompletion";
        private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";
        private static readonly ILog Log = LogManager.GetLogger<MongoDbJobStore>();
        private static long _fireTriggerRecordCounter = DateTime.UtcNow.Ticks;
        private CalendarRepository _calendarRepository;
        private IMongoClient _client;
        private IMongoDatabase _database;
        private FiredTriggerRepository _firedTriggerRepository;
        private JobDetailRepository _jobDetailRepository;
        private LockManager _lockManager;
        private PausedTriggerGroupRepository _pausedTriggerGroupRepository;
        private SchedulerId _schedulerId;
        private SchedulerRepository _schedulerRepository;
        private TriggerRepository _triggerRepository;
        private MisfireHandler _misfireHandler;

        private ISchedulerSignaler _schedulerSignaler;
        private TimeSpan _misfireThreshold = TimeSpan.FromMinutes(1);
        private bool _schedulerRunning;

        static MongoDbJobStore()
        {
            JobStoreClassMap.RegisterClassMaps();
        }

        public string ConnectionString { get; set; }
        public string CollectionPrefix { get; set; }

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;
        public bool Clustered => false;
        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        /// <summary>
        /// Get or set the maximum number of misfired triggers that the misfire handling
        /// thread will try to recover at one time (within one transaction).  The
        /// default is 20.
        /// </summary>
        public int MaxMisfiresToHandleAtATime { get; set; }

        /// <summary>
        /// Gets or sets the database retry interval.
        /// </summary>
        /// <value>The db retry interval.</value>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan DbRetryInterval { get; set; }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireThreshold
        {
            get { return _misfireThreshold; }
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }
                _misfireThreshold = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of retries before an error is logged for recovery operations.
        /// </summary>
        public int RetryableActionErrorLogThreshold { get; set; }

        protected DateTimeOffset MisfireTime
        {
            get
            {
                var misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1*MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        public MongoDbJobStore()
        {
            MaxMisfiresToHandleAtATime = 20;
            RetryableActionErrorLogThreshold = 4;
            DbRetryInterval = TimeSpan.FromSeconds(15);
        }

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken token = default(CancellationToken))
        {
            _schedulerSignaler = signaler;
            _schedulerId = new SchedulerId(InstanceId, InstanceName);
            Log.Trace($"Scheduler {_schedulerId} initialize");

            var url = new MongoUrl(ConnectionString);
            _client = new MongoClient(ConnectionString);
            _database = _client.GetDatabase(url.DatabaseName);
            _lockManager = new LockManager(_database, InstanceName, CollectionPrefix);
            _schedulerRepository = new SchedulerRepository(_database, InstanceName, CollectionPrefix);
            _jobDetailRepository = new JobDetailRepository(_database, InstanceName, CollectionPrefix);
            _triggerRepository = new TriggerRepository(_database, InstanceName, CollectionPrefix);
            _pausedTriggerGroupRepository = new PausedTriggerGroupRepository(_database, InstanceName, CollectionPrefix);
            _firedTriggerRepository = new FiredTriggerRepository(_database, InstanceName, CollectionPrefix);
            _calendarRepository = new CalendarRepository(_database, InstanceName, CollectionPrefix);

            return Task.FromResult(true);
        }

        public Task SchedulerStarted(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} started");
            _schedulerRepository.AddScheduler(new Scheduler
            {
                Id = _schedulerId,
                State = SchedulerState.Started,
                LastCheckIn = DateTime.Now
            });

            try
            {
                RecoverJobs();
            }
            catch (Exception ex)
            {
                throw new SchedulerConfigException("Failure occurred during job recovery", ex);
            }
            _misfireHandler = new MisfireHandler(this);
            _misfireHandler.Start();
            _schedulerRunning = true;
            return Task.FromResult(true);
        }

        public Task SchedulerPaused(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} paused");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Paused);
            _schedulerRunning = false;
            return Task.FromResult(true);
        }

        public Task SchedulerResumed(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} resumed");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Resumed);
            _schedulerRunning = true;
            return Task.FromResult(true);
        }

        public Task Shutdown(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} shutdown");
            if (_misfireHandler != null)
            {
                _misfireHandler.Shutdown();
                try
                {
                    _misfireHandler.Join();
                }
                catch (ThreadInterruptedException)
                {
                }
            }
            _schedulerRepository.DeleteScheduler(_schedulerId.Id);
            return Task.FromResult(true);
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    StoreJobInternal(newJob, false);
                    await StoreTriggerInternal(newTrigger, newJob, false, Models.TriggerState.Waiting, false, false,
                        cancellationToken);
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken token = default(CancellationToken))
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken token = default(CancellationToken))
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    StoreJobInternal(newJob, replaceExisting);
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    foreach (var job in triggersAndJobs.Keys)
                    {
                        StoreJobInternal(job, replace);
                        foreach (var trigger in triggersAndJobs[job])
                        {
                            await StoreTriggerInternal((IOperableTrigger) trigger, job, replace, Models.TriggerState.Waiting, false, false, cancellationToken);
                        }
                    }
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> RemoveJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(RemoveJobInternal(jobKey));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(jobKeys.Aggregate(true, (current, jobKey) => current && RemoveJobInternal(jobKey)));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_jobDetailRepository.GetJob(jobKey)?.GetJobDetail());
        }

        public Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default(CancellationToken))
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return StoreTriggerInternal(newTrigger, null, replaceExisting, Models.TriggerState.Waiting, false, false, cancellationToken);
            }
        }

        public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(RemoveTriggerInternal(triggerKey));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(triggerKeys.Aggregate(true, (current, triggerKey) => current && RemoveTriggerInternal(triggerKey)));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(ReplaceTriggerInternal(triggerKey, newTrigger));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_triggerRepository.GetTrigger(triggerKey)?.GetTrigger() as IOperableTrigger);
        }

        public Task<bool> CalendarExists(string calName, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_calendarRepository.CalendarExists(calName));
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_jobDetailRepository.JobExists(jobKey));
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_triggerRepository.TriggerExists(triggerKey));
        }

        public Task ClearAllSchedulingData(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    _calendarRepository.DeleteAll();
                    _firedTriggerRepository.DeleteAll();
                    _jobDetailRepository.DeleteAll();
                    _pausedTriggerGroupRepository.DeleteAll();
                    _schedulerRepository.DeleteAll();
                    _triggerRepository.DeleteAll();
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return StoreCalendarInternal(name, calendar, replaceExisting, updateTriggers, token);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> RemoveCalendar(string calName, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return Task.FromResult(RemoveCalendarInternal(calName));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<ICalendar> RetrieveCalendar(string calName, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult(_calendarRepository.GetCalendar(calName)?.GetCalendar());
        }

        public Task<int> GetNumberOfJobs(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((int) _jobDetailRepository.GetCount());
        }

        public Task<int> GetNumberOfTriggers(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((int) _triggerRepository.GetCount());
        }

        public Task<int> GetNumberOfCalendars(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((int) _calendarRepository.GetCount());
        }

        public Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<JobKey>)new HashSet<JobKey>(_jobDetailRepository.GetJobsKeys(matcher)));
        }

        public Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<TriggerKey>)new HashSet<TriggerKey>(_triggerRepository.GetTriggerKeys(matcher)));
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<string>)_jobDetailRepository.GetJobGroupNames().ToList());
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<string>)_triggerRepository.GetTriggerGroupNames().ToList());
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<string>)_calendarRepository.GetCalendarNames().ToList());
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<IOperableTrigger>)_triggerRepository.GetTriggers(jobKey)
                .Select(trigger => trigger.GetTrigger())
                .Cast<IOperableTrigger>()
                .ToList());
        }

        public Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);

            if (trigger == null)
            {
                return Task.FromResult(TriggerState.None);
            }

            switch (trigger.State)
            {
                case Models.TriggerState.Deleted:
                    return Task.FromResult(TriggerState.None);
                case Models.TriggerState.Complete:
                    return Task.FromResult(TriggerState.Complete);
                case Models.TriggerState.Paused:
                case Models.TriggerState.PausedBlocked:
                    return Task.FromResult(TriggerState.Paused);
                case Models.TriggerState.Error:
                    return Task.FromResult(TriggerState.Error);
                case Models.TriggerState.Blocked:
                    return Task.FromResult(TriggerState.Blocked);
                default:
                    return Task.FromResult(TriggerState.Normal);
            }
        }

        public Task PauseTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    PauseTriggerInternal(triggerKey);
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return PauseTriggerGroupInternal(matcher, token);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task PauseJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    var triggers = GetTriggersForJob(jobKey, token);
                    foreach (var operableTrigger in triggers.Result)
                    {
                        PauseTriggerInternal(operableTrigger.Key);
                    }

                    return Task.FromResult(true);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    var jobKeys = _jobDetailRepository.GetJobsKeys(matcher);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = _triggerRepository.GetTriggers(jobKey);
                        foreach (var trigger in triggers)
                        {
                            PauseTriggerInternal(trigger.GetTrigger().Key);
                        }
                    }
                    return Task.FromResult((IReadOnlyCollection<string>)jobKeys.Select(key => key.Group).Distinct().ToList());
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return ResumeTriggerInternal(triggerKey, token);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return ResumeTriggersInternal(matcher, token);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult((IReadOnlyCollection<string>)new HashSet<string>(_pausedTriggerGroupRepository.GetPausedTriggerGroups()));
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    var triggers = _triggerRepository.GetTriggers(jobKey);
                    await Task.WhenAll(triggers.Select(trigger =>
                        ResumeTriggerInternal(trigger.GetTrigger().Key, cancellationToken)));
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    var jobKeys = _jobDetailRepository.GetJobsKeys(matcher);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = _triggerRepository.GetTriggers(jobKey);
                        await Task.WhenAll(triggers.Select(trigger => ResumeTriggerInternal(trigger.GetTrigger().Key, cancellationToken)));
                    }
                    return (IReadOnlyCollection<string>)new HashSet<string>(jobKeys.Select(key => key.Group));
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task PauseAll(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    PauseAllInternal();
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task ResumeAll(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    ResumeAllInternal();
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    return AcquireNextTriggersInternal(noLaterThan, maxCount, timeWindow);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Waiting,
                        Models.TriggerState.Acquired);
                    _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId);
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    var results = new List<TriggerFiredResult>();

                    foreach (var operableTrigger in triggers)
                    {
                        TriggerFiredResult result;
                        try
                        {
                            var bundle = TriggerFiredInternal(operableTrigger);
                            result = new TriggerFiredResult(bundle);
                        }
                        catch (Exception ex)
                        {
                            Log.Error($"Caught exception: {ex.Message}", ex);
                            result = new TriggerFiredResult(ex);
                        }
                        results.Add(result);
                    }

                    return Task.FromResult((IReadOnlyCollection<TriggerFiredResult>)results);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                {
                    TriggeredJobCompleteInternal(trigger, jobDetail, triggerInstCode, token).Wait(token);
                }

                var sigTime = ClearAndGetSignalSchedulingChangeOnTxCompletion();
                if (sigTime != null)
                {
                    SignalSchedulingChangeImmediately(sigTime);
                }

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        internal RecoverMisfiredJobsResult DoRecoverMisfires()
        {
            try
            {
                var result = RecoverMisfiredJobsResult.NoOp;

                var misfireCount = _triggerRepository.GetMisfireCount(MisfireTime.UtcDateTime);
                if (misfireCount == 0)
                {
                    Log.Debug("Found 0 triggers that missed their scheduled fire-time.");
                }
                else
                {
                    using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
                    {
                        result = RecoverMisfiredJobsInternal(false);
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        private void RecoverJobs()
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                RecoverJobsInternal();
            }
        }

        private void PauseTriggerInternal(TriggerKey triggerKey)
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);
            switch (trigger.State)
            {
                case Models.TriggerState.Waiting:
                case Models.TriggerState.Acquired:
                    _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Paused);
                    break;
                case Models.TriggerState.Blocked:
                    _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.PausedBlocked);
                    break;
            }
        }

        private Task<IReadOnlyCollection<string>> PauseTriggerGroupInternal(GroupMatcher<TriggerKey> matcher, CancellationToken token = default(CancellationToken))
        {
            _triggerRepository.UpdateTriggersStates(matcher, Models.TriggerState.Paused, Models.TriggerState.Acquired,
                Models.TriggerState.Waiting);
            _triggerRepository.UpdateTriggersStates(matcher, Models.TriggerState.PausedBlocked,
                Models.TriggerState.Blocked);

            var triggerGroups = _triggerRepository.GetTriggerGroupNames(matcher).ToList();

            // make sure to account for an exact group match for a group that doesn't yet exist
            var op = matcher.CompareWithOperator;
            if (op.Equals(StringOperator.Equality) && !triggerGroups.Contains(matcher.CompareToValue))
            {
                triggerGroups.Add(matcher.CompareToValue);
            }

            foreach (var triggerGroup in triggerGroups)
            {
                if (!_pausedTriggerGroupRepository.IsTriggerGroupPaused(triggerGroup))
                {
                    _pausedTriggerGroupRepository.AddPausedTriggerGroup(triggerGroup);
                }
            }

            return Task.FromResult((IReadOnlyCollection<string>)new HashSet<string>(triggerGroups));
        }

        private void PauseAllInternal()
        {
            var groupNames = _triggerRepository.GetTriggerGroupNames();

            Task.WhenAll(groupNames.Select(groupName => PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))).Wait();
            
            if (!_pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused))
            {
                _pausedTriggerGroupRepository.AddPausedTriggerGroup(AllGroupsPaused);
            }
        }

        private bool ReplaceTriggerInternal(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);
            var job = _jobDetailRepository.GetJob(trigger.JobKey)?.GetJobDetail();

            if (job == null)
            {
                return false;
            }

            if (!newTrigger.JobKey.Equals(job.Key))
            {
                throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
            }

            var removedTrigger = _triggerRepository.DeleteTrigger(triggerKey);
            StoreTriggerInternal(newTrigger, job, false, Models.TriggerState.Waiting, false, false).Wait();
            return removedTrigger > 0;
        }

        private bool RemoveJobInternal(JobKey jobKey)
        {
            _triggerRepository.DeleteTriggers(jobKey);
            var result = _jobDetailRepository.DeleteJob(jobKey);
            return result > 0;
        }

        private bool RemoveTriggerInternal(TriggerKey key, IJobDetail job = null)
        {
            var trigger = _triggerRepository.GetTrigger(key);
            if (trigger == null)
            {
                return false;
            }

            if (job == null)
            {
                job = _jobDetailRepository.GetJob(trigger.JobKey)?.GetJobDetail();
            }

            var removedTrigger = _triggerRepository.DeleteTrigger(key) > 0;

            if (job != null && !job.Durable)
            {
                if (_triggerRepository.GetCount(job.Key) == 0)
                {
                    if (RemoveJobInternal(job.Key))
                    {
                        _schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key).Wait();
                    }
                }
            }

            return removedTrigger;
        }

        private bool RemoveCalendarInternal(string calendarName)
        {
            if (_triggerRepository.TriggersExists(calendarName))
            {
                throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
            }

            return _calendarRepository.DeleteCalendar(calendarName) > 0;
        }

        private Task ResumeTriggerInternal(TriggerKey triggerKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);
            if (trigger?.NextFireTime == null || trigger.NextFireTime == DateTime.MinValue)
            {
                return Task.FromResult(true);
            }

            var blocked = trigger.State == Models.TriggerState.PausedBlocked;
            var newState = CheckBlockedState(trigger.JobKey, Models.TriggerState.Waiting);
            var misfired = false;

            if (_schedulerRunning && trigger.NextFireTime < DateTime.UtcNow)
            {
                misfired = UpdateMisfiredTrigger(triggerKey, newState, true);
            }

            if (!misfired)
            {
                _triggerRepository.UpdateTriggerState(triggerKey, newState,
                    blocked ? Models.TriggerState.PausedBlocked : Models.TriggerState.Paused);
            }

            return Task.FromResult(true);
        }

        private async Task<IReadOnlyCollection<string>> ResumeTriggersInternal(GroupMatcher<TriggerKey> matcher, CancellationToken token = default(CancellationToken))
        {
            _pausedTriggerGroupRepository.DeletePausedTriggerGroup(matcher);
            var groups = new HashSet<string>();

            var keys = _triggerRepository.GetTriggerKeys(matcher);
            foreach (var triggerKey in keys)
            {
                await ResumeTriggerInternal(triggerKey, token);
                groups.Add(triggerKey.Group);
            }
            return groups.ToList();
        }

        private void ResumeAllInternal()
        {
            var groupNames = _triggerRepository.GetTriggerGroupNames();
            Task.WhenAll(groupNames.Select(groupName => ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))).Wait();
            _pausedTriggerGroupRepository.DeletePausedTriggerGroup(AllGroupsPaused);
        }

        private Task StoreCalendarInternal(string calName, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken token = default(CancellationToken))
        {
            var existingCal = CalendarExists(calName, token).Result;
            if (existingCal && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException("Calendar with name '" + calName + "' already exists.");
            }

            if (existingCal)
            {
                if (_calendarRepository.UpdateCalendar(new Calendar(calName, calendar, InstanceName)) == 0)
                {
                    throw new JobPersistenceException("Couldn't store calendar.  Update failed.");
                }

                if (updateTriggers)
                {
                    var triggers = _triggerRepository.GetTriggers(calName);
                    foreach (var trigger in triggers)
                    {
                        var quartzTrigger = (IOperableTrigger) trigger.GetTrigger();
                        quartzTrigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        StoreTriggerInternal(quartzTrigger, null, true, Models.TriggerState.Waiting, false, false, token).Wait(token);
                    }
                }
            }
            else
            {
                _calendarRepository.AddCalendar(new Calendar(calName, calendar, InstanceName));
            }

            return Task.FromResult(true);
        }

        private void StoreJobInternal(IJobDetail newJob, bool replaceExisting)
        {
            var existingJob = _jobDetailRepository.JobExists(newJob.Key);

            if (existingJob)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
                _jobDetailRepository.UpdateJob(new JobDetail(newJob, InstanceName), true);
            }
            else
            {
                _jobDetailRepository.AddJob(new JobDetail(newJob, InstanceName));
            }
        }

        private Task StoreTriggerInternal(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting,
            Models.TriggerState state, bool forceState, bool recovering, CancellationToken token = default(CancellationToken))
        {
            var existingTrigger = _triggerRepository.TriggerExists(newTrigger.Key);

            if (existingTrigger && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            if (!forceState)
            {
                var shouldBePaused =
                    _pausedTriggerGroupRepository.IsTriggerGroupPaused(newTrigger.Key.Group);

                if (!shouldBePaused)
                {
                    shouldBePaused = _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused);
                    if (shouldBePaused)
                    {
                        _pausedTriggerGroupRepository.AddPausedTriggerGroup(newTrigger.Key.Group);
                    }
                }

                if (shouldBePaused &&
                    state == Models.TriggerState.Waiting || state == Models.TriggerState.Acquired)
                {
                    state = Models.TriggerState.Paused;
                }
            }

            if (job == null)
            {
                job = _jobDetailRepository.GetJob(newTrigger.JobKey)?.GetJobDetail();
            }

            if (job == null)
            {
                throw new JobPersistenceException(
                    $"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
            }

            if (job.ConcurrentExecutionDisallowed && !recovering)
            {
                state = CheckBlockedState(job.Key, state);
            }

            if (existingTrigger)
            {
                _triggerRepository.UpdateTrigger(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName));
            }
            else
            {
                _triggerRepository.AddTrigger(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName));
            }

            return Task.FromResult(true);
        }

        private Models.TriggerState CheckBlockedState(JobKey jobKey, Models.TriggerState currentState)
        {
            if (currentState != Models.TriggerState.Waiting && currentState != Models.TriggerState.Paused)
            {
                return currentState;
            }

            var firedTrigger = _firedTriggerRepository.GetFiredTriggers(jobKey).FirstOrDefault();
            if (firedTrigger != null)
            {
                if (firedTrigger.ConcurrentExecutionDisallowed)
                {
                    return currentState == Models.TriggerState.Paused
                        ? Models.TriggerState.PausedBlocked
                        : Models.TriggerState.Blocked;
                }
            }

            return currentState;
        }

        private TriggerFiredBundle TriggerFiredInternal(IOperableTrigger trigger)
        {
            var state = _triggerRepository.GetTriggerState(trigger.Key);
            if (state != Models.TriggerState.Acquired)
            {
                return null;
            }

            var job = _jobDetailRepository.GetJob(trigger.JobKey);
            if (job == null)
            {
                return null;
            }

            ICalendar calendar = null;
            if (trigger.CalendarName != null)
            {
                calendar = _calendarRepository.GetCalendar(trigger.CalendarName)?.GetCalendar();
                if (calendar == null)
                {
                    return null;
                }
            }

            _firedTriggerRepository.UpdateFiredTrigger(
                new FiredTrigger(trigger.FireInstanceId,
                    TriggerFactory.CreateTrigger(trigger, Models.TriggerState.Executing, InstanceName), job)
                {
                    InstanceId = InstanceId,
                    State = Models.TriggerState.Executing
                });

            var prevFireTime = trigger.GetPreviousFireTimeUtc();
            trigger.Triggered(calendar);

            state = Models.TriggerState.Waiting;
            var force = true;

            if (job.ConcurrentExecutionDisallowed)
            {
                state = Models.TriggerState.Blocked;
                force = false;
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked,
                    Models.TriggerState.Waiting);
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked,
                    Models.TriggerState.Acquired);
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.PausedBlocked,
                    Models.TriggerState.Paused);
            }

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                state = Models.TriggerState.Complete;
                force = true;
            }

            var jobDetail = job.GetJobDetail();
            StoreTriggerInternal(trigger, jobDetail, true, state, force, force).Wait();

            jobDetail.JobDataMap.ClearDirtyFlag();

            return new TriggerFiredBundle(jobDetail,
                trigger,
                calendar,
                trigger.Key.Group.Equals(SchedulerConstants.DefaultRecoveryGroup),
                DateTimeOffset.UtcNow,
                trigger.GetPreviousFireTimeUtc(),
                prevFireTime,
                trigger.GetNextFireTimeUtc());
        }

        private bool UpdateMisfiredTrigger(TriggerKey triggerKey, Models.TriggerState newStateIfNotComplete,
            bool forceState)
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);
            var misfireTime = DateTime.Now;
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1*MisfireThreshold.TotalMilliseconds);
            }

            if (trigger.NextFireTime > misfireTime)
            {
                return false;
            }
            DoUpdateOfMisfiredTrigger(trigger, forceState, newStateIfNotComplete, false);

            return true;
        }

        private void DoUpdateOfMisfiredTrigger(Trigger trigger, bool forceState,
            Models.TriggerState newStateIfNotComplete, bool recovering)
        {
            var operableTrigger = (IOperableTrigger) trigger.GetTrigger();

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = _calendarRepository.GetCalendar(trigger.CalendarName).GetCalendar();
            }

            _schedulerSignaler.NotifyTriggerListenersMisfired(operableTrigger).Wait();
            operableTrigger.UpdateAfterMisfire(cal);

            if (operableTrigger.GetNextFireTimeUtc().HasValue)
            {
                StoreTriggerInternal(operableTrigger, null, true, Models.TriggerState.Complete, forceState, recovering).Wait();
                _schedulerSignaler.NotifySchedulerListenersFinalized(operableTrigger).Wait();
            }
            else
            {
                StoreTriggerInternal(operableTrigger, null, true, newStateIfNotComplete, forceState, false).Wait();
            }
        }

        private Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersInternal(DateTimeOffset noLaterThan, int maxCount,
            TimeSpan timeWindow)
        {
            if (timeWindow < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeWindow));
            }

            var acquiredTriggers = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

            const int maxDoLoopRetry = 3;
            var currentLoopCount = 0;

            do
            {
                currentLoopCount++;
                var keys = _triggerRepository.GetTriggersToAcquire(noLaterThan + timeWindow, MisfireTime, maxCount);

                if (!keys.Any())
                {
                    return Task.FromResult((IReadOnlyCollection<IOperableTrigger>)acquiredTriggers);
                }

                foreach (var triggerKey in keys)
                {
                    var nextTrigger = _triggerRepository.GetTrigger(triggerKey);
                    if (nextTrigger == null)
                    {
                        continue;
                    }

                    var jobKey = nextTrigger.JobKey;
                    JobDetail jobDetail;
                    try
                    {
                        jobDetail = _jobDetailRepository.GetJob(jobKey);
                    }
                    catch (Exception)
                    {
                        _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Error);
                        continue;
                    }

                    if (jobDetail.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue;
                        }
                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }

                    var result = _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Acquired,
                        Models.TriggerState.Waiting);
                    if (result <= 0)
                    {
                        continue;
                    }

                    var operableTrigger = (IOperableTrigger) nextTrigger.GetTrigger();
                    operableTrigger.FireInstanceId = GetFiredTriggerRecordId();

                    var firedTrigger = new FiredTrigger(operableTrigger.FireInstanceId, nextTrigger, null)
                    {
                        State = Models.TriggerState.Acquired,
                        InstanceId = InstanceId
                    };
                    _firedTriggerRepository.AddFiredTrigger(firedTrigger);

                    acquiredTriggers.Add(operableTrigger);
                }

                if (acquiredTriggers.Count == 0 && currentLoopCount < maxDoLoopRetry)
                {
                    continue;
                }

                break;
            } while (true);

            return Task.FromResult((IReadOnlyCollection<IOperableTrigger>)acquiredTriggers);
        }

        private string GetFiredTriggerRecordId()
        {
            Interlocked.Increment(ref _fireTriggerRecordCounter);
            return InstanceId + _fireTriggerRecordCounter;
        }

        private Task TriggeredJobCompleteInternal(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode, CancellationToken token = default(CancellationToken))
        {
            try
            {
                switch (triggerInstCode)
                {
                    case SchedulerInstruction.DeleteTrigger:
                        if (!trigger.GetNextFireTimeUtc().HasValue)
                        {
                            var trig = _triggerRepository.GetTrigger(trigger.Key);
                            if (trig != null && !trig.NextFireTime.HasValue)
                            {
                                RemoveTriggerInternal(trigger.Key, jobDetail);
                            }
                        }
                        else
                        {
                            RemoveTriggerInternal(trigger.Key, jobDetail);
                            SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        }
                        break;
                    case SchedulerInstruction.SetTriggerComplete:
                        _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Complete);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetTriggerError:
                        Log.Info("Trigger " + trigger.Key + " set to ERROR state.");
                        _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Error);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersComplete:
                        _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Complete);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersError:
                        Log.Info("All triggers of Job " + trigger.JobKey + " set to ERROR state.");
                        _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Error);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Waiting,
                        Models.TriggerState.Blocked);
                    _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Paused,
                        Models.TriggerState.PausedBlocked);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                }

                if (jobDetail.PersistJobDataAfterExecution && jobDetail.JobDataMap.Dirty)
                {
                    _jobDetailRepository.UpdateJobData(jobDetail.Key, jobDetail.JobDataMap);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }

            try
            {
                _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId);
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        protected virtual void SignalSchedulingChangeOnTxCompletion(DateTimeOffset? candidateNewNextFireTime)
        {
            var sigTime = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
            if (sigTime == null && candidateNewNextFireTime.HasValue)
            {
                LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
            }
            else
            {
                if (sigTime == null || candidateNewNextFireTime < sigTime)
                {
                    LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
                }
            }
        }

        protected virtual DateTimeOffset? ClearAndGetSignalSchedulingChangeOnTxCompletion()
        {
            var t = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
            LogicalThreadContext.FreeNamedDataSlot(KeySignalChangeForTxCompletion);
            return t;
        }

        internal virtual void SignalSchedulingChangeImmediately(DateTimeOffset? candidateNewNextFireTime)
        {
            _schedulerSignaler.SignalSchedulingChange(candidateNewNextFireTime);
        }

        private void RecoverJobsInternal()
        {
            var result = _triggerRepository.UpdateTriggersStates(Models.TriggerState.Waiting,
                Models.TriggerState.Acquired, Models.TriggerState.Blocked);
            result += _triggerRepository.UpdateTriggersStates(Models.TriggerState.Paused,
                Models.TriggerState.PausedBlocked);

            Log.Info("Freed " + result + " triggers from 'acquired' / 'blocked' state.");

            RecoverMisfiredJobsInternal(true);

            var recoveringJobTriggers = _firedTriggerRepository.GetRecoverableFiredTriggers(InstanceId)
                .Select(
                    trigger => trigger.GetRecoveryTrigger(_triggerRepository.GetTriggerJobDataMap(trigger.TriggerKey)))
                .ToList();
            Log.Info("Recovering " + recoveringJobTriggers.Count +
                     " jobs that were in-progress at the time of the last shut-down.");

            foreach (var recoveringJobTrigger in recoveringJobTriggers)
            {
                if (_jobDetailRepository.JobExists(recoveringJobTrigger.JobKey))
                {
                    recoveringJobTrigger.ComputeFirstFireTimeUtc(null);
                    StoreTriggerInternal(recoveringJobTrigger, null, false, Models.TriggerState.Waiting, false, true).Wait();
                }
            }
            Log.Info("Recovery complete");

            var completedTriggers = _triggerRepository.GetTriggerKeys(Models.TriggerState.Complete);
            foreach (var completedTrigger in completedTriggers)
            {
                RemoveTriggerInternal(completedTrigger);
            }

            Log.Info(string.Format(CultureInfo.InvariantCulture, "Removed {0} 'complete' triggers.",
                completedTriggers.Count));

            result = _firedTriggerRepository.DeleteFiredTriggersByInstanceId(InstanceId);
            Log.Info("Removed " + result + " stale fired job entries.");
        }

        private RecoverMisfiredJobsResult RecoverMisfiredJobsInternal(bool recovering)
        {
            var maxMisfiresToHandleAtTime = (recovering) ? -1 : MaxMisfiresToHandleAtATime;
            List<TriggerKey> misfiredTriggers;
            var earliestNewTime = DateTime.MaxValue;

            var hasMoreMisfiredTriggers = _triggerRepository.HasMisfiredTriggers(MisfireTime.UtcDateTime,
                maxMisfiresToHandleAtTime, out misfiredTriggers);

            if (hasMoreMisfiredTriggers)
            {
                Log.Info(
                    "Handling the first " + misfiredTriggers.Count +
                    " triggers that missed their scheduled fire-time.  " +
                    "More misfired triggers remain to be processed.");
            }
            else if (misfiredTriggers.Count > 0)
            {
                Log.Info(
                    "Handling " + misfiredTriggers.Count +
                    " trigger(s) that missed their scheduled fire-time.");
            }
            else
            {
                Log.Debug(
                    "Found 0 triggers that missed their scheduled fire-time.");
                return RecoverMisfiredJobsResult.NoOp;
            }

            foreach (var misfiredTrigger in misfiredTriggers)
            {
                var trigger = _triggerRepository.GetTrigger(misfiredTrigger);

                if (trigger == null)
                {
                    continue;
                }

                DoUpdateOfMisfiredTrigger(trigger, false, Models.TriggerState.Waiting, recovering);

                var nextTime = trigger.NextFireTime;
                if (nextTime.HasValue && nextTime.Value < earliestNewTime)
                {
                    earliestNewTime = nextTime.Value;
                }
            }

            return new RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.Count,
                earliestNewTime);
        }
    }
}
