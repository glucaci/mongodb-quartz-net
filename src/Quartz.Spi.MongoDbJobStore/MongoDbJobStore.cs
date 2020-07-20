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
        private const string KeySignalChangeForTxCompletion = "sigChangeForTxCompletion";
        private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";

        private static readonly DateTimeOffset? SchedulingSignalDateTime = new DateTimeOffset(1982, 6, 28, 0, 0, 0,
            TimeSpan.FromSeconds(0));

        private static readonly ILog Log = LogManager.GetLogger<MongoDbJobStore>();
        private static long _fireTriggerRecordCounter = DateTime.UtcNow.Ticks;
        private CalendarRepository _calendarRepository;
        private IMongoClient _client;
        private IMongoDatabase _database;
        private FiredTriggerRepository _firedTriggerRepository;
        private JobDetailRepository _jobDetailRepository;
        private LockManager _lockManager;
        private MisfireHandler _misfireHandler;
        private TimeSpan _misfireThreshold = TimeSpan.FromMinutes(1);
        private PausedTriggerGroupRepository _pausedTriggerGroupRepository;
        private SchedulerId _schedulerId;
        private SchedulerRepository _schedulerRepository;
        private bool _schedulerRunning;

        private ISchedulerSignaler _schedulerSignaler;
        private TriggerRepository _triggerRepository;

        static MongoDbJobStore()
        {
            JobStoreClassMap.RegisterClassMaps();
        }

        public MongoDbJobStore()
        {
            MaxMisfiresToHandleAtATime = 20;
            RetryableActionErrorLogThreshold = 4;
            DbRetryInterval = TimeSpan.FromSeconds(15);
        }

        public string ConnectionString { get; set; }
        public string CollectionPrefix { get; set; }

        /// <summary>
        ///     Get or set the maximum number of misfired triggers that the misfire handling
        ///     thread will try to recover at one time (within one transaction).  The
        ///     default is 20.
        /// </summary>
        public int MaxMisfiresToHandleAtATime { get; set; }

        /// <summary>
        ///     Gets or sets the database retry interval.
        /// </summary>
        /// <value>The db retry interval.</value>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan DbRetryInterval { get; set; }

        /// <summary>
        ///     The time span by which a trigger must have missed its
        ///     next-fire-time, in order for it to be considered "misfired" and thus
        ///     have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireThreshold
        {
            get => _misfireThreshold;
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
        ///     Gets or sets the number of retries before an error is logged for recovery operations.
        /// </summary>
        public int RetryableActionErrorLogThreshold { get; set; }

        protected DateTimeOffset MisfireTime
        {
            get
            {
                var misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;
        public bool Clustered => false;
        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler,
            CancellationToken token = default(CancellationToken))
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

        public async Task SchedulerStarted(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} started");
            await _schedulerRepository.AddScheduler(new Scheduler
            {
                Id = _schedulerId,
                State = SchedulerState.Started,
                LastCheckIn = DateTime.Now
            }).ConfigureAwait(false);

            try
            {
                await RecoverJobs().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new SchedulerConfigException("Failure occurred during job recovery", ex);
            }

            _misfireHandler = new MisfireHandler(this);
            _misfireHandler.Start();
            _schedulerRunning = true;
        }

        public async Task SchedulerPaused(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} paused");
            await _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Paused).ConfigureAwait(false);
            _schedulerRunning = false;
        }

        public async Task SchedulerResumed(CancellationToken token = default(CancellationToken))
        {
            Log.Trace($"Scheduler {_schedulerId} resumed");
            await _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Resumed).ConfigureAwait(false);
            _schedulerRunning = true;
        }

        public async Task Shutdown(CancellationToken token = default(CancellationToken))
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

            await _schedulerRepository.DeleteScheduler(_schedulerId.Id).ConfigureAwait(false);
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await StoreJobInternal(newJob, false).ConfigureAwait(false);
                    await StoreTriggerInternal(newTrigger, newJob, false, Models.TriggerState.Waiting, false, false,
                        cancellationToken).ConfigureAwait(false);
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

        public async Task StoreJob(IJobDetail newJob, bool replaceExisting,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await StoreJobInternal(newJob, replaceExisting).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    foreach (var job in triggersAndJobs.Keys)
                    {
                        await StoreJobInternal(job, replace).ConfigureAwait(false);
                        foreach (var trigger in triggersAndJobs[job])
                            await StoreTriggerInternal((IOperableTrigger) trigger, job, replace,
                                Models.TriggerState.Waiting, false, false, cancellationToken).ConfigureAwait(false);
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

        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await RemoveJobInternal(jobKey).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return jobKeys.Aggregate(true, (current, jobKey) => current && RemoveJobInternal(jobKey).Result);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            var result = await _jobDetailRepository.GetJob(jobKey).ConfigureAwait(false);
            return result?.GetJobDetail();
        }

        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
            {
                await StoreTriggerInternal(newTrigger, null, replaceExisting, Models.TriggerState.Waiting, false, false,
                    cancellationToken);
            }
        }

        public async Task<bool> RemoveTrigger(TriggerKey triggerKey,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await RemoveTriggerInternal(triggerKey).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return triggerKeys.Aggregate(true,
                        (current, triggerKey) => current && RemoveTriggerInternal(triggerKey).Result);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await ReplaceTriggerInternal(triggerKey, newTrigger).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey,
            CancellationToken token = default(CancellationToken))
        {
            var result = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
            return result?.GetTrigger() as IOperableTrigger;
        }

        public async Task<bool> CalendarExists(string calName, CancellationToken token = default(CancellationToken))
        {
            return await _calendarRepository.CalendarExists(calName).ConfigureAwait(false);
        }

        public async Task<bool> CheckExists(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            return await _jobDetailRepository.JobExists(jobKey).ConfigureAwait(false);
        }

        public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            return await _triggerRepository.TriggerExists(triggerKey).ConfigureAwait(false);
        }

        public async Task ClearAllSchedulingData(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await _calendarRepository.DeleteAll().ConfigureAwait(false);
                    await _firedTriggerRepository.DeleteAll().ConfigureAwait(false);
                    await _jobDetailRepository.DeleteAll().ConfigureAwait(false);
                    await _pausedTriggerGroupRepository.DeleteAll().ConfigureAwait(false);
                    await _schedulerRepository.DeleteAll().ConfigureAwait(false);
                    await _triggerRepository.DeleteAll().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await StoreCalendarInternal(name, calendar, replaceExisting, updateTriggers, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveCalendar(string calName, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await RemoveCalendarInternal(calName).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<ICalendar> RetrieveCalendar(string calName,
            CancellationToken token = default(CancellationToken))
        {
            var result = await _calendarRepository.GetCalendar(calName).ConfigureAwait(false);
            return result?.GetCalendar();
        }

        public async Task<int> GetNumberOfJobs(CancellationToken token = default(CancellationToken))
        {
            return (int) await _jobDetailRepository.GetCount().ConfigureAwait(false);
        }

        public async Task<int> GetNumberOfTriggers(CancellationToken token = default(CancellationToken))
        {
            return (int) await _triggerRepository.GetCount().ConfigureAwait(false);
        }

        public async Task<int> GetNumberOfCalendars(CancellationToken token = default(CancellationToken))
        {
            return (int) await _calendarRepository.GetCount().ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            return (IReadOnlyCollection<JobKey>) new HashSet<JobKey>(await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false)
                );
        }

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            return (IReadOnlyCollection<TriggerKey>) new HashSet<TriggerKey>(await _triggerRepository
                .GetTriggerKeys(matcher).ConfigureAwait(false));
        }

        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(
            CancellationToken token = default(CancellationToken))
        {
            return (IReadOnlyCollection<string>) await _jobDetailRepository.GetJobGroupNames().ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            CancellationToken token = default(CancellationToken))
        {
            return await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<string>> GetCalendarNames(
            CancellationToken token = default(CancellationToken))
        {
            return (IReadOnlyCollection<string>) await _calendarRepository.GetCalendarNames().ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey,
            CancellationToken token = default(CancellationToken))
        {
            var result = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);
            return result.Select(trigger => trigger.GetTrigger())
                .Cast<IOperableTrigger>()
                .ToList();
        }

        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey,
            CancellationToken token = default(CancellationToken))
        {
            var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case Models.TriggerState.Deleted:
                    return TriggerState.None;
                case Models.TriggerState.Complete:
                    return TriggerState.Complete;
                case Models.TriggerState.Paused:
                case Models.TriggerState.PausedBlocked:
                    return TriggerState.Paused;
                case Models.TriggerState.Error:
                    return TriggerState.Error;
                case Models.TriggerState.Blocked:
                    return TriggerState.Blocked;
                default:
                    return TriggerState.Normal;
            }
        }

        public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await PauseTriggerInternal(triggerKey).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await PauseTriggerGroupInternal(matcher, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task PauseJob(JobKey jobKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    var triggers = await GetTriggersForJob(jobKey, token).ConfigureAwait(false);
                    foreach (var operableTrigger in triggers)
                        await PauseTriggerInternal(operableTrigger.Key).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);
                        foreach (var trigger in triggers)
                            await PauseTriggerInternal(trigger.GetTrigger().Key).ConfigureAwait(false);
                    }

                    return jobKeys.Select(key => key.Group).Distinct().ToList();
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await ResumeTriggerInternal(triggerKey, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await ResumeTriggersInternal(matcher, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return (IReadOnlyCollection<string>) new HashSet<string>(await _pausedTriggerGroupRepository
                .GetPausedTriggerGroups().ConfigureAwait(false));
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);
                    await Task.WhenAll(triggers.Select(trigger =>
                        ResumeTriggerInternal(trigger.GetTrigger().Key, cancellationToken))).ConfigureAwait(false);
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

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);
                        await Task.WhenAll(triggers.Select(trigger =>
                            ResumeTriggerInternal(trigger.GetTrigger().Key, cancellationToken))).ConfigureAwait(false);
                    }

                    return (IReadOnlyCollection<string>) new HashSet<string>(jobKeys.Select(key => key.Group));
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

        public async Task PauseAll(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await PauseAllInternal().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ResumeAll(CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await ResumeAllInternal().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan,
            int maxCount, TimeSpan timeWindow, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    return await AcquireNextTriggersInternal(noLaterThan, maxCount, timeWindow).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger,
            CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Waiting,
                        Models.TriggerState.Acquired).ConfigureAwait(false);
                    await _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    var results = new List<TriggerFiredResult>();

                    foreach (var operableTrigger in triggers)
                    {
                        TriggerFiredResult result;
                        try
                        {
                            var bundle = await TriggerFiredInternal(operableTrigger).ConfigureAwait(false);
                            result = new TriggerFiredResult(bundle);
                        }
                        catch (Exception ex)
                        {
                            Log.Error($"Caught exception: {ex.Message}", ex);
                            result = new TriggerFiredResult(ex);
                        }

                        results.Add(result);
                    }

                    return results;
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode, CancellationToken token = default(CancellationToken))
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                {
                    await TriggeredJobCompleteInternal(trigger, jobDetail, triggerInstCode, token).ConfigureAwait(false)
                        ;
                }

                var sigTime = ClearAndGetSignalSchedulingChangeOnTxCompletion();
                if (sigTime != null)
                {
                    SignalSchedulingChangeImmediately(sigTime);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        internal async Task<RecoverMisfiredJobsResult> DoRecoverMisfires()
        {
            try
            {
                var result = RecoverMisfiredJobsResult.NoOp;

                var misfireCount =
                    await _triggerRepository.GetMisfireCount(MisfireTime.UtcDateTime).ConfigureAwait(false);
                if (misfireCount == 0)
                {
                    Log.Debug("Found 0 triggers that missed their scheduled fire-time.");
                }
                else
                {
                    using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
                    {
                        result = await RecoverMisfiredJobsInternal(false).ConfigureAwait(false);
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        private async Task RecoverJobs()
        {
            using (await _lockManager.AcquireLock(LockType.TriggerAccess, InstanceId).ConfigureAwait(false))
            {
                await RecoverJobsInternal().ConfigureAwait(false);
            }
        }

        private async Task PauseTriggerInternal(TriggerKey triggerKey)
        {
            var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
            switch (trigger.State)
            {
                case Models.TriggerState.Waiting:
                case Models.TriggerState.Acquired:
                    await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Paused).ConfigureAwait(false)
                        ;
                    break;
                case Models.TriggerState.Blocked:
                    await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.PausedBlocked).ConfigureAwait(false)
                        ;
                    break;
            }
        }

        private async Task<IReadOnlyCollection<string>> PauseTriggerGroupInternal(GroupMatcher<TriggerKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            await _triggerRepository.UpdateTriggersStates(matcher, Models.TriggerState.Paused,
                Models.TriggerState.Acquired,
                Models.TriggerState.Waiting).ConfigureAwait(false);
            await _triggerRepository.UpdateTriggersStates(matcher, Models.TriggerState.PausedBlocked,
                Models.TriggerState.Blocked).ConfigureAwait(false);

            var triggerGroups = await _triggerRepository.GetTriggerGroupNames(matcher).ConfigureAwait(false);

            // make sure to account for an exact group match for a group that doesn't yet exist
            var op = matcher.CompareWithOperator;
            if (op.Equals(StringOperator.Equality) && !triggerGroups.Contains(matcher.CompareToValue))
            {
                triggerGroups.Add(matcher.CompareToValue);
            }

            foreach (var triggerGroup in triggerGroups)
                if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(triggerGroup).ConfigureAwait(false))
                {
                    await _pausedTriggerGroupRepository.AddPausedTriggerGroup(triggerGroup).ConfigureAwait(false);
                }

            return (IReadOnlyCollection<string>) new HashSet<string>(triggerGroups);
        }

        private async Task PauseAllInternal()
        {
            var groupNames = await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);

            await Task.WhenAll(groupNames.Select(groupName =>
                PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))).ConfigureAwait(false);

            if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused).ConfigureAwait(false))
            {
                await _pausedTriggerGroupRepository.AddPausedTriggerGroup(AllGroupsPaused).ConfigureAwait(false);
            }
        }

        private async Task<bool> ReplaceTriggerInternal(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
            var result = await _jobDetailRepository.GetJob(trigger.JobKey).ConfigureAwait(false);
            var job = result?.GetJobDetail();

            if (job == null)
            {
                return false;
            }

            if (!newTrigger.JobKey.Equals(job.Key))
            {
                throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
            }

            var removedTrigger = await _triggerRepository.DeleteTrigger(triggerKey).ConfigureAwait(false);
            await StoreTriggerInternal(newTrigger, job, false, Models.TriggerState.Waiting, false, false).ConfigureAwait(false)
                ;
            return removedTrigger > 0;
        }

        private async Task<bool> RemoveJobInternal(JobKey jobKey)
        {
            await _triggerRepository.DeleteTriggers(jobKey).ConfigureAwait(false);
            var result = await _jobDetailRepository.DeleteJob(jobKey).ConfigureAwait(false);
            return result > 0;
        }

        private async Task<bool> RemoveTriggerInternal(TriggerKey key, IJobDetail job = null)
        {
            var trigger = await _triggerRepository.GetTrigger(key);
            if (trigger == null)
            {
                return false;
            }

            if (job == null)
            {
                var result = await _jobDetailRepository.GetJob(trigger.JobKey).ConfigureAwait(false);
                job = result?.GetJobDetail();
            }

            var removedTrigger = await _triggerRepository.DeleteTrigger(key).ConfigureAwait(false) > 0;

            if (job != null && !job.Durable)
            {
                if (await _triggerRepository.GetCount(job.Key).ConfigureAwait(false) == 0)
                {
                    if (await RemoveJobInternal(job.Key).ConfigureAwait(false))
                    {
                        await _schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key).ConfigureAwait(false);
                    }
                }
            }

            return removedTrigger;
        }

        private async Task<bool> RemoveCalendarInternal(string calendarName)
        {
            if (await _triggerRepository.TriggersExists(calendarName).ConfigureAwait(false))
            {
                throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
            }

            return await _calendarRepository.DeleteCalendar(calendarName).ConfigureAwait(false) > 0;
        }

        private async Task ResumeTriggerInternal(TriggerKey triggerKey,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
            if (trigger?.NextFireTime == null || trigger.NextFireTime == DateTime.MinValue)
            {
                return;
            }

            var blocked = trigger.State == Models.TriggerState.PausedBlocked;
            var newState = await CheckBlockedState(trigger.JobKey, Models.TriggerState.Waiting).ConfigureAwait(false);
            var misfired = false;

            if (_schedulerRunning && trigger.NextFireTime < DateTime.UtcNow)
            {
                misfired = await UpdateMisfiredTrigger(triggerKey, newState, true).ConfigureAwait(false);
            }

            if (!misfired)
            {
                await _triggerRepository.UpdateTriggerState(triggerKey, newState,
                    blocked ? Models.TriggerState.PausedBlocked : Models.TriggerState.Paused).ConfigureAwait(false);
            }
        }

        private async Task<IReadOnlyCollection<string>> ResumeTriggersInternal(GroupMatcher<TriggerKey> matcher,
            CancellationToken token = default(CancellationToken))
        {
            await _pausedTriggerGroupRepository.DeletePausedTriggerGroup(matcher).ConfigureAwait(false);
            var groups = new HashSet<string>();

            var keys = await _triggerRepository.GetTriggerKeys(matcher).ConfigureAwait(false);
            foreach (var triggerKey in keys)
            {
                await ResumeTriggerInternal(triggerKey, token).ConfigureAwait(false);
                groups.Add(triggerKey.Group);
            }

            return groups.ToList();
        }

        private async Task ResumeAllInternal()
        {
            var groupNames = await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);
            await Task.WhenAll(groupNames.Select(groupName =>
                ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))).ConfigureAwait(false);
            await _pausedTriggerGroupRepository.DeletePausedTriggerGroup(AllGroupsPaused).ConfigureAwait(false);
        }

        private async Task StoreCalendarInternal(string calName, ICalendar calendar, bool replaceExisting,
            bool updateTriggers, CancellationToken token = default(CancellationToken))
        {
            var existingCal = await CalendarExists(calName, token).ConfigureAwait(false);
            if (existingCal && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException("Calendar with name '" + calName + "' already exists.");
            }

            if (existingCal)
            {
                if (await _calendarRepository.UpdateCalendar(new Calendar(calName, calendar, InstanceName)).ConfigureAwait(false)
                         == 0)
                {
                    throw new JobPersistenceException("Couldn't store calendar.  Update failed.");
                }

                if (updateTriggers)
                {
                    var triggers = await _triggerRepository.GetTriggers(calName).ConfigureAwait(false);
                    foreach (var trigger in triggers)
                    {
                        var quartzTrigger = (IOperableTrigger) trigger.GetTrigger();
                        quartzTrigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                        await StoreTriggerInternal(quartzTrigger, null, true, Models.TriggerState.Waiting, false, false,
                            token).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                await _calendarRepository.AddCalendar(new Calendar(calName, calendar, InstanceName)).ConfigureAwait(false)
                    ;
            }
        }

        private async Task StoreJobInternal(IJobDetail newJob, bool replaceExisting)
        {
            var existingJob = await _jobDetailRepository.JobExists(newJob.Key).ConfigureAwait(false);

            if (existingJob)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }

                await _jobDetailRepository.UpdateJob(new JobDetail(newJob, InstanceName), true).ConfigureAwait(false);
            }
            else
            {
                await _jobDetailRepository.AddJob(new JobDetail(newJob, InstanceName)).ConfigureAwait(false);
            }
        }

        private async Task StoreTriggerInternal(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting,
            Models.TriggerState state, bool forceState, bool recovering,
            CancellationToken token = default(CancellationToken))
        {
            var existingTrigger = await _triggerRepository.TriggerExists(newTrigger.Key).ConfigureAwait(false);

            if (existingTrigger && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            if (!forceState)
            {
                var shouldBePaused =
                    await _pausedTriggerGroupRepository.IsTriggerGroupPaused(newTrigger.Key.Group).ConfigureAwait(false)
                        ;

                if (!shouldBePaused)
                {
                    shouldBePaused = await _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused).ConfigureAwait(false)
                        ;
                    if (shouldBePaused)
                    {
                        await _pausedTriggerGroupRepository.AddPausedTriggerGroup(newTrigger.Key.Group).ConfigureAwait(false)
                            ;
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
                job = (await _jobDetailRepository.GetJob(newTrigger.JobKey).ConfigureAwait(false))?.GetJobDetail();
            }

            if (job == null)
            {
                throw new JobPersistenceException(
                    $"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
            }

            if (job.ConcurrentExecutionDisallowed && !recovering)
            {
                state = await CheckBlockedState(job.Key, state).ConfigureAwait(false);
            }

            if (existingTrigger)
            {
                await _triggerRepository.UpdateTrigger(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName)).ConfigureAwait(false)
                    ;
            }
            else
            {
                await _triggerRepository.AddTrigger(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName)).ConfigureAwait(false)
                    ;
            }
        }

        private async Task<Models.TriggerState> CheckBlockedState(JobKey jobKey, Models.TriggerState currentState)
        {
            if (currentState != Models.TriggerState.Waiting && currentState != Models.TriggerState.Paused)
            {
                return currentState;
            }

            var firedTrigger = (await _firedTriggerRepository.GetFiredTriggers(jobKey).ConfigureAwait(false))
                .FirstOrDefault();
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

        private async Task<TriggerFiredBundle> TriggerFiredInternal(IOperableTrigger trigger)
        {
            var state = await _triggerRepository.GetTriggerState(trigger.Key).ConfigureAwait(false);
            if (state != Models.TriggerState.Acquired)
            {
                return null;
            }

            var job = await _jobDetailRepository.GetJob(trigger.JobKey).ConfigureAwait(false);
            if (job == null)
            {
                return null;
            }

            ICalendar calendar = null;
            if (trigger.CalendarName != null)
            {
                calendar = (await _calendarRepository.GetCalendar(trigger.CalendarName).ConfigureAwait(false))
                    ?.GetCalendar();
                if (calendar == null)
                {
                    return null;
                }
            }

            await _firedTriggerRepository.UpdateFiredTrigger(
                new FiredTrigger(trigger.FireInstanceId,
                    TriggerFactory.CreateTrigger(trigger, Models.TriggerState.Executing, InstanceName), job)
                {
                    InstanceId = InstanceId,
                    State = Models.TriggerState.Executing
                }).ConfigureAwait(false);

            var prevFireTime = trigger.GetPreviousFireTimeUtc();
            trigger.Triggered(calendar);

            state = Models.TriggerState.Waiting;
            var force = true;

            if (job.ConcurrentExecutionDisallowed)
            {
                state = Models.TriggerState.Blocked;
                force = false;
                await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked,
                    Models.TriggerState.Waiting).ConfigureAwait(false);
                await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked,
                    Models.TriggerState.Acquired).ConfigureAwait(false);
                await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.PausedBlocked,
                    Models.TriggerState.Paused).ConfigureAwait(false);
            }

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                state = Models.TriggerState.Complete;
                force = true;
            }

            var jobDetail = job.GetJobDetail();
            await StoreTriggerInternal(trigger, jobDetail, true, state, force, force).ConfigureAwait(false);

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

        private async Task<bool> UpdateMisfiredTrigger(TriggerKey triggerKey, Models.TriggerState newStateIfNotComplete,
            bool forceState)
        {
            var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
            var misfireTime = DateTime.Now;
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            if (trigger.NextFireTime > misfireTime)
            {
                return false;
            }

            await DoUpdateOfMisfiredTrigger(trigger, forceState, newStateIfNotComplete, false).ConfigureAwait(false);

            return true;
        }

        private async Task DoUpdateOfMisfiredTrigger(Trigger trigger, bool forceState,
            Models.TriggerState newStateIfNotComplete, bool recovering)
        {
            var operableTrigger = (IOperableTrigger) trigger.GetTrigger();

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = (await _calendarRepository.GetCalendar(trigger.CalendarName).ConfigureAwait(false)).GetCalendar();
            }

            await _schedulerSignaler.NotifyTriggerListenersMisfired(operableTrigger).ConfigureAwait(false);
            operableTrigger.UpdateAfterMisfire(cal);

            if (!operableTrigger.GetNextFireTimeUtc().HasValue)
            {
                await StoreTriggerInternal(operableTrigger, null, true, Models.TriggerState.Complete, forceState,
                    recovering).ConfigureAwait(false);
                await _schedulerSignaler.NotifySchedulerListenersFinalized(operableTrigger).ConfigureAwait(false);
            }
            else
            {
                await StoreTriggerInternal(operableTrigger, null, true, newStateIfNotComplete, forceState, false).ConfigureAwait(false);
            }
        }

        private async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersInternal(
            DateTimeOffset noLaterThan, int maxCount,
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
                var keys = await _triggerRepository
                    .GetTriggersToAcquire(noLaterThan + timeWindow, MisfireTime, maxCount).ConfigureAwait(false);

                if (!keys.Any())
                {
                    return acquiredTriggers;
                }

                foreach (var triggerKey in keys)
                {
                    var nextTrigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
                    if (nextTrigger == null)
                    {
                        continue;
                    }

                    var jobKey = nextTrigger.JobKey;
                    JobDetail jobDetail;
                    try
                    {
                        jobDetail = await _jobDetailRepository.GetJob(jobKey).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Error).ConfigureAwait(false)
                            ;
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

                    var result = await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Acquired,
                        Models.TriggerState.Waiting).ConfigureAwait(false);
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
                    await _firedTriggerRepository.AddFiredTrigger(firedTrigger).ConfigureAwait(false);

                    acquiredTriggers.Add(operableTrigger);
                }

                if (acquiredTriggers.Count == 0 && currentLoopCount < maxDoLoopRetry)
                {
                    continue;
                }

                break;
            } while (true);

            return acquiredTriggers;
        }

        private string GetFiredTriggerRecordId()
        {
            Interlocked.Increment(ref _fireTriggerRecordCounter);
            return InstanceId + _fireTriggerRecordCounter;
        }

        private async Task TriggeredJobCompleteInternal(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode, CancellationToken token = default(CancellationToken))
        {
            try
            {
                switch (triggerInstCode)
                {
                    case SchedulerInstruction.DeleteTrigger:
                        if (!trigger.GetNextFireTimeUtc().HasValue)
                        {
                            var trig = await _triggerRepository.GetTrigger(trigger.Key).ConfigureAwait(false);
                            if (trig != null && !trig.NextFireTime.HasValue)
                            {
                                await RemoveTriggerInternal(trigger.Key, jobDetail).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            await RemoveTriggerInternal(trigger.Key, jobDetail).ConfigureAwait(false);
                            SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        }

                        break;
                    case SchedulerInstruction.SetTriggerComplete:
                        await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Complete).ConfigureAwait(false)
                            ;
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetTriggerError:
                        Log.Info("Trigger " + trigger.Key + " set to ERROR state.");
                        await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Error).ConfigureAwait(false)
                            ;
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersComplete:
                        await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Complete).ConfigureAwait(false)
                            ;
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersError:
                        Log.Info("All triggers of Job " + trigger.JobKey + " set to ERROR state.");
                        await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Error).ConfigureAwait(false)
                            ;
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    await _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Waiting,
                        Models.TriggerState.Blocked).ConfigureAwait(false);
                    await _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Paused,
                        Models.TriggerState.PausedBlocked).ConfigureAwait(false);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                }

                if (jobDetail.PersistJobDataAfterExecution && jobDetail.JobDataMap.Dirty)
                {
                    await _jobDetailRepository.UpdateJobData(jobDetail.Key, jobDetail.JobDataMap).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }

            try
            {
                await _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId).ConfigureAwait(false);
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

        private async Task RecoverJobsInternal()
        {
            var result = await _triggerRepository.UpdateTriggersStates(Models.TriggerState.Waiting,
                Models.TriggerState.Acquired, Models.TriggerState.Blocked).ConfigureAwait(false);
            result += await _triggerRepository.UpdateTriggersStates(Models.TriggerState.Paused,
                Models.TriggerState.PausedBlocked).ConfigureAwait(false);

            Log.Info("Freed " + result + " triggers from 'acquired' / 'blocked' state.");

            await RecoverMisfiredJobsInternal(true).ConfigureAwait(false);

            var results = (await _firedTriggerRepository.GetRecoverableFiredTriggers(InstanceId).ConfigureAwait(false))
                .Select(async trigger =>
                    trigger.GetRecoveryTrigger(await _triggerRepository.GetTriggerJobDataMap(trigger.TriggerKey).ConfigureAwait(false)));
            var recoveringJobTriggers = (await Task.WhenAll(results).ConfigureAwait(false)).ToList();

            Log.Info("Recovering " + recoveringJobTriggers.Count +
                     " jobs that were in-progress at the time of the last shut-down.");

            foreach (var recoveringJobTrigger in recoveringJobTriggers)
                if (await _jobDetailRepository.JobExists(recoveringJobTrigger.JobKey).ConfigureAwait(false))
                {
                    recoveringJobTrigger.ComputeFirstFireTimeUtc(null);
                    await StoreTriggerInternal(recoveringJobTrigger, null, false, Models.TriggerState.Waiting, false,
                        true).ConfigureAwait(false);
                }

            Log.Info("Recovery complete");

            var completedTriggers =
                await _triggerRepository.GetTriggerKeys(Models.TriggerState.Complete).ConfigureAwait(false);
            foreach (var completedTrigger in completedTriggers)
                await RemoveTriggerInternal(completedTrigger).ConfigureAwait(false);

            Log.Info(string.Format(CultureInfo.InvariantCulture, "Removed {0} 'complete' triggers.",
                completedTriggers.Count));

            result = await _firedTriggerRepository.DeleteFiredTriggersByInstanceId(InstanceId).ConfigureAwait(false);
            Log.Info("Removed " + result + " stale fired job entries.");
        }

        private async Task<RecoverMisfiredJobsResult> RecoverMisfiredJobsInternal(bool recovering)
        {
            var maxMisfiresToHandleAtTime = recovering ? -1 : MaxMisfiresToHandleAtATime;
            var earliestNewTime = DateTime.MaxValue;

            var hasMoreMisfiredTriggers = _triggerRepository.HasMisfiredTriggers(MisfireTime.UtcDateTime,
                maxMisfiresToHandleAtTime, out var misfiredTriggers);

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
                var trigger = await _triggerRepository.GetTrigger(misfiredTrigger).ConfigureAwait(false);

                if (trigger == null)
                {
                    continue;
                }

                await DoUpdateOfMisfiredTrigger(trigger, false, Models.TriggerState.Waiting, recovering).ConfigureAwait(false)
                    ;

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