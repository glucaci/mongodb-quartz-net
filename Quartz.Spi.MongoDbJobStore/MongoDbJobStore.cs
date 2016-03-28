using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
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
        private static readonly DateTimeOffset? SchedulingSignalDateTime = new DateTimeOffset(1982, 6, 28, 0, 0, 0, TimeSpan.FromSeconds(0));
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

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
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
        }

        public void SchedulerStarted()
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
        }

        public void SchedulerPaused()
        {
            Log.Trace($"Scheduler {_schedulerId} paused");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Paused);
            _schedulerRunning = false;
        }

        public void SchedulerResumed()
        {
            Log.Trace($"Scheduler {_schedulerId} resumed");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Resumed);
            _schedulerRunning = true;
        }

        public void Shutdown()
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
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                StoreJobInternal(newJob, false);
                StoreTriggerInternal(newTrigger, newJob, false, Models.TriggerState.Waiting, false, false);
            }
        }

        public bool IsJobGroupPaused(string groupName)
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                StoreJobInternal(newJob, replaceExisting);
            }
        }

        public void StoreJobsAndTriggers(
            System.Collections.Generic.IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs,
            bool replace)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                foreach (var job in triggersAndJobs.Keys)
                {
                    StoreJobInternal(job, replace);
                    foreach (var trigger in triggersAndJobs[job])
                    {
                        StoreTriggerInternal((IOperableTrigger) trigger, job, replace, Models.TriggerState.Waiting,
                            false, false);
                    }
                }
            }
        }

        public bool RemoveJob(JobKey jobKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return RemoveJobInternal(jobKey);
            }
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return jobKeys.Aggregate(true, (current, jobKey) => current && RemoveJobInternal(jobKey));
            }
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            return _jobDetailRepository.GetJob(jobKey)?.GetJobDetail();
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                StoreTriggerInternal(newTrigger, null, replaceExisting, Models.TriggerState.Waiting, false, false);
            }
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return RemoveTriggerInternal(triggerKey);
            }
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return triggerKeys.Aggregate(true, (current, triggerKey) => current && RemoveTriggerInternal(triggerKey));
            }
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return ReplaceTriggerInternal(triggerKey, newTrigger);
            }
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            return _triggerRepository.GetTrigger(triggerKey)?.GetTrigger() as IOperableTrigger;
        }

        public bool CalendarExists(string calName)
        {
            return _calendarRepository.CalendarExists(calName);
        }

        public bool CheckExists(JobKey jobKey)
        {
            return _jobDetailRepository.JobExists(jobKey);
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            return _triggerRepository.TriggerExists(triggerKey);
        }

        public void ClearAllSchedulingData()
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
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                StoreCalendarInternal(name, calendar, replaceExisting, updateTriggers);
            }
        }

        public bool RemoveCalendar(string calName)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return RemoveCalendarInternal(calName);
            }
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            return _calendarRepository.GetCalendar(calName)?.GetCalendar();
        }

        public int GetNumberOfJobs()
        {
            return (int) _jobDetailRepository.GetCount();
        }

        public int GetNumberOfTriggers()
        {
            return (int) _triggerRepository.GetCount();
        }

        public int GetNumberOfCalendars()
        {
            return (int) _calendarRepository.GetCount();
        }

        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            return new Collection.HashSet<JobKey>(_jobDetailRepository.GetJobsKeys(matcher));
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            return new Collection.HashSet<TriggerKey>(_triggerRepository.GetTriggerKeys(matcher));
        }

        public IList<string> GetJobGroupNames()
        {
            return _jobDetailRepository.GetJobGroupNames().ToList();
        }

        public IList<string> GetTriggerGroupNames()
        {
            return _triggerRepository.GetTriggerGroupNames().ToList();
        }

        public IList<string> GetCalendarNames()
        {
            return _calendarRepository.GetCalendarNames().ToList();
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            return _triggerRepository.GetTriggers(jobKey)
                .Select(trigger => trigger.GetTrigger())
                .Cast<IOperableTrigger>()
                .ToList();
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);

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

        public void PauseTrigger(TriggerKey triggerKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                PauseTriggerInternal(triggerKey);
            }
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return PauseTriggerGroupInternal(matcher);
            }
        }

        public void PauseJob(JobKey jobKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                var triggers = GetTriggersForJob(jobKey);
                foreach (var operableTrigger in triggers)
                {
                    PauseTriggerInternal(operableTrigger.Key);
                }
            }
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
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
                return jobKeys.Select(key => key.Group).Distinct().ToList();
            }
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                ResumeTriggerInternal(triggerKey);
            }
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return ResumeTriggersInternal(matcher);
            }
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            return new Collection.HashSet<string>(_pausedTriggerGroupRepository.GetPausedTriggerGroups());
        }

        public void ResumeJob(JobKey jobKey)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                var triggers = _triggerRepository.GetTriggers(jobKey);
                foreach (var trigger in triggers)
                {
                    ResumeTriggerInternal(trigger.GetTrigger().Key);
                }
            }
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                var jobKeys = _jobDetailRepository.GetJobsKeys(matcher);
                foreach (var jobKey in jobKeys)
                {
                    var triggers = _triggerRepository.GetTriggers(jobKey);
                    foreach (var trigger in triggers)
                    {
                        ResumeTriggerInternal(trigger.GetTrigger().Key);
                    }
                }
                return new Collection.HashSet<string>(jobKeys.Select(key => key.Group));
            }
        }

        public void PauseAll()
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                PauseAllInternal();
            }
        }

        public void ResumeAll()
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                ResumeAllInternal();
            }
        }

        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                return AcquireNextTriggersInternal(noLaterThan, maxCount, timeWindow);
            }
        }

        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Waiting,
                    Models.TriggerState.Acquired);
                _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId);
            }
        }

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
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

                return results;
            }
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                TriggeredJobCompleteInternal(trigger, jobDetail, triggerInstCode);
            }

            var sigTime = ClearAndGetSignalSchedulingChangeOnTxCompletion();
            if (sigTime != null)
            {
                SignalSchedulingChangeImmediately(sigTime);
            }
        }

        internal JobStoreSupport.RecoverMisfiredJobsResult DoRecoverMisfires()
        {
            try
            {
                var result = JobStoreSupport.RecoverMisfiredJobsResult.NoOp;

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

        private Collection.ISet<string> PauseTriggerGroupInternal(GroupMatcher<TriggerKey> matcher)
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

            return new Collection.HashSet<string>(triggerGroups);
        }

        private void PauseAllInternal()
        {
            var groupNames = _triggerRepository.GetTriggerGroupNames();
            foreach (var groupName in groupNames)
            {
                PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName));
            }

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

            var removedTrigger = RemoveTriggerInternal(triggerKey);
            StoreTriggerInternal(newTrigger, job, false, Models.TriggerState.Waiting, false, false);
            return removedTrigger;
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
                        _schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key);
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

        private void ResumeTriggerInternal(TriggerKey triggerKey)
        {
            var trigger = _triggerRepository.GetTrigger(triggerKey);
            if (trigger?.NextFireTime == null || trigger.NextFireTime == DateTime.MinValue)
            {
                return;
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
        }

        private IList<string> ResumeTriggersInternal(GroupMatcher<TriggerKey> matcher)
        {
            _pausedTriggerGroupRepository.DeletePausedTriggerGroup(matcher);
            var groups = new HashSet<string>();

            var keys = _triggerRepository.GetTriggerKeys(matcher);
            foreach (var triggerKey in keys)
            {
                ResumeTriggerInternal(triggerKey);
                groups.Add(triggerKey.Group);
            }
            return groups.ToList();
        }

        private void ResumeAllInternal()
        {
            var groupNames = _triggerRepository.GetTriggerGroupNames();
            foreach (var groupName in groupNames)
            {
                ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName));
            }

            _pausedTriggerGroupRepository.DeletePausedTriggerGroup(AllGroupsPaused);
        }

        private void StoreCalendarInternal(string calName, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var existingCal = CalendarExists(calName);
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
                        StoreTriggerInternal(quartzTrigger, null, true, Models.TriggerState.Waiting, false, false);
                    }
                }
            }
            else
            {
                _calendarRepository.AddCalendar(new Calendar(calName, calendar, InstanceName));
            }
        }

        private void StoreJobInternal(IJobDetail newJob, bool replaceExisting)
        {
            if (replaceExisting)
            {
                var result = _jobDetailRepository.UpdateJob(new JobDetail(newJob, InstanceName), true);
                if (result == 0)
                {
                    throw new JobPersistenceException("Could not store job");
                }
            }
            else
            {
                _jobDetailRepository.AddJob(new JobDetail(newJob, InstanceName));
            }
        }

        private void StoreTriggerInternal(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting,
            Models.TriggerState state, bool forceState, bool recovering)
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
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked, Models.TriggerState.Waiting);
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Blocked, Models.TriggerState.Acquired);
                _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.PausedBlocked, Models.TriggerState.Paused);
            }

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                state = Models.TriggerState.Complete;
                force = true;
            }

            var jobDetail = job.GetJobDetail();
            StoreTriggerInternal(trigger, jobDetail, true, state, force, force);

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

            _schedulerSignaler.NotifyTriggerListenersMisfired(operableTrigger);
            operableTrigger.UpdateAfterMisfire(cal);

            if (operableTrigger.GetNextFireTimeUtc().HasValue)
            {
                StoreTriggerInternal(operableTrigger, null, true, Models.TriggerState.Complete, forceState, recovering);
                _schedulerSignaler.NotifySchedulerListenersFinalized(operableTrigger);
            }
            else
            {
                StoreTriggerInternal(operableTrigger, null, true, newStateIfNotComplete, forceState, false);
            }
        }

        private IList<IOperableTrigger> AcquireNextTriggersInternal(DateTimeOffset noLaterThan, int maxCount,
            TimeSpan timeWindow)
        {
            if (timeWindow < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeWindow));
            }

            var acquiredTriggers = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();

            const int maxDoLoopRetry = 3;
            var currentLoopCount = 0;

            do
            {
                currentLoopCount++;
                var keys = _triggerRepository.GetTriggersToAcquire(noLaterThan + timeWindow, MisfireTime, maxCount);

                if (!keys.Any())
                {
                    return acquiredTriggers;
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

            return acquiredTriggers;
        }

        private string GetFiredTriggerRecordId()
        {
            Interlocked.Increment(ref _fireTriggerRecordCounter);
            return InstanceId + _fireTriggerRecordCounter;
        }

        private void TriggeredJobCompleteInternal(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode)
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
                    _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Waiting, Models.TriggerState.Blocked);
                    _triggerRepository.UpdateTriggersStates(jobDetail.Key, Models.TriggerState.Paused, Models.TriggerState.PausedBlocked);
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
            var result = _triggerRepository.UpdateTriggersStates(Models.TriggerState.Waiting, Models.TriggerState.Acquired, Models.TriggerState.Blocked);
            result += _triggerRepository.UpdateTriggersStates(Models.TriggerState.Paused,
                Models.TriggerState.PausedBlocked);

            Log.Info("Freed " + result + " triggers from 'acquired' / 'blocked' state.");

            RecoverMisfiredJobsInternal(true);

            var recoveringJobTriggers = _firedTriggerRepository.GetRecoverableFiredTriggers(InstanceId)
                .Select(trigger => trigger.GetRecoveryTrigger(_triggerRepository.GetTriggerJobDataMap(trigger.TriggerKey)))
                .ToList();
            Log.Info("Recovering " + recoveringJobTriggers.Count +
                        " jobs that were in-progress at the time of the last shut-down.");

            foreach (var recoveringJobTrigger in recoveringJobTriggers)
            {
                if (_jobDetailRepository.JobExists(recoveringJobTrigger.JobKey))
                {
                    recoveringJobTrigger.ComputeFirstFireTimeUtc(null);
                    StoreTriggerInternal(recoveringJobTrigger, null, false, Models.TriggerState.Waiting, false, true);
                }
            }
            Log.Info("Recovery complete");

            var completedTriggers = _triggerRepository.GetTriggerKeys(Models.TriggerState.Complete);
            foreach (var completedTrigger in completedTriggers)
            {
                RemoveTriggerInternal(completedTrigger);
            }

            Log.Info(string.Format(CultureInfo.InvariantCulture, "Removed {0} 'complete' triggers.", completedTriggers.Count));

            result = _firedTriggerRepository.DeleteFiredTriggersByInstanceId(InstanceId);
            Log.Info("Removed " + result + " stale fired job entries.");
        }

        private JobStoreSupport.RecoverMisfiredJobsResult RecoverMisfiredJobsInternal(bool recovering)
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
                return JobStoreSupport.RecoverMisfiredJobsResult.NoOp;
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

            return new JobStoreSupport.RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.Count, earliestNewTime);
        }
    }
}