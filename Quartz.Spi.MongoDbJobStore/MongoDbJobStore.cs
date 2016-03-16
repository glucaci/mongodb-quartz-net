using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;
using Quartz.Spi.MongoDbJobStore.Repositories;

namespace Quartz.Spi.MongoDbJobStore
{
    public class MongoDbJobStore : IJobStore
    {
        private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";
        private static readonly ILog Log = LogManager.GetLogger<MongoDbJobStore>();
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

        private ISchedulerSignaler _schedulerSignaler;
        private TimeSpan _misfireThreshold = TimeSpan.FromMinutes(1);

        static MongoDbJobStore()
        {
            JobStoreClassMap.RegisterClassMaps();
        }

        public string ConnectionString { get; set; }
        public string CollectionPrefix { get; set; }

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;
        public bool Clustered => true;
        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
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
            // TODO: Recover jobs
        }

        public void SchedulerPaused()
        {
            Log.Trace($"Scheduler {_schedulerId} paused");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Paused);
        }

        public void SchedulerResumed()
        {
            Log.Trace($"Scheduler {_schedulerId} resumed");
            _schedulerRepository.UpdateState(_schedulerId.Id, SchedulerState.Resumed);
        }

        public void Shutdown()
        {
            Log.Trace($"Scheduler {_schedulerId} shutdown");
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

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs,
            bool replace)
        {
            using (_lockManager.AcquireLock(LockType.TriggerAccess, InstanceId))
            {
                foreach (var job in triggersAndJobs.Keys)
                {
                    StoreJobInternal(job, replace);
                    foreach (var trigger in triggersAndJobs[job])
                    {
                        StoreTriggerInternal((IOperableTrigger)trigger, job, replace, Models.TriggerState.Waiting, false, false);
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
            throw new NotImplementedException();
        }

        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public IList<string> GetJobGroupNames()
        {
            throw new NotImplementedException();
        }

        public IList<string> GetTriggerGroupNames()
        {
            throw new NotImplementedException();
        }

        public IList<string> GetCalendarNames()
        {
            throw new NotImplementedException();
        }

        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public void PauseTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void PauseJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void ResumeTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            throw new NotImplementedException();
        }

        public void ResumeJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            throw new NotImplementedException();
        }

        public void PauseAll()
        {
            throw new NotImplementedException();
        }

        public void ResumeAll()
        {
            throw new NotImplementedException();
        }

        public IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            throw new NotImplementedException();
        }

        public void ReleaseAcquiredTrigger(IOperableTrigger trigger)
        {
            throw new NotImplementedException();
        }

        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            throw new NotImplementedException();
        }

        public void TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode)
        {
            throw new NotImplementedException();
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
                        var quartzTrigger = (IOperableTrigger)trigger.GetTrigger();
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
            if (currentState == Models.TriggerState.Waiting || currentState == Models.TriggerState.Paused)
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
    }
}