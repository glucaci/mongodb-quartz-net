using System;
using System.Collections.Generic;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Repositories;

namespace Quartz.Spi.MongoDbJobStore
{
    public class MongoDbJobStore : IJobStore
    {
        private static readonly ILog Log = LogManager.GetLogger<MongoDbJobStore>();

        private IMongoClient _client;
        private IMongoDatabase _database;
        private LockManager _lockManager;
        private SchedulerRepository _schedulerRepository;
        private JobDetailRepository _jobDetailRepository;
        private TriggerRepository _triggerRepository;

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 200;
        public bool Clustered => true;
        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public string ConnectionString { get; set; }

        static MongoDbJobStore()
        {
            JobStoreClassMap.RegisterClassMaps();
        }

        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler)
        {
            Log.Trace($"Scheduler {InstanceName}/{InstanceId} initialize");
            var url = new MongoUrl(ConnectionString);
            _client = new MongoClient(ConnectionString);
            _database = _client.GetDatabase(url.DatabaseName);
            _lockManager = new LockManager(_database, InstanceName);
            _schedulerRepository = new SchedulerRepository(_database, InstanceName);
            _jobDetailRepository = new JobDetailRepository(_database, InstanceName);
            _triggerRepository = new TriggerRepository(_database, InstanceName);
        }

        public void SchedulerStarted()
        {
            Log.Trace($"Scheduler {InstanceName}/{InstanceId} started");
            _schedulerRepository.AddScheduler(new Scheduler()
            {
                Id = InstanceId,
                State = SchedulerState.Started,
                LastCheckIn = DateTime.Now

            });
        }

        public void SchedulerPaused()
        {
            Log.Trace($"Scheduler {InstanceName}/{InstanceId} paused");
            _schedulerRepository.UpdateState(InstanceId, SchedulerState.Paused);
        }

        public void SchedulerResumed()
        {
            Log.Trace($"Scheduler {InstanceName}/{InstanceId} resumed");
            _schedulerRepository.UpdateState(InstanceId, SchedulerState.Resumed);
        }

        public void Shutdown()
        {
            Log.Trace($"Scheduler {InstanceName}/{InstanceId} shutdown");
            _schedulerRepository.DeleteScheduler(InstanceId);
        }

        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public bool IsJobGroupPaused(string groupName)
        {
            throw new NotImplementedException();
        }

        public bool IsTriggerGroupPaused(string groupName)
        {
            throw new NotImplementedException();
        }

        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            using (_lockManager.AcquireLock(Lock.TriggerAccess, InstanceId))
            {
                if (replaceExisting)
                {
                    var result = _jobDetailRepository.UpdateJobDetail(new JobDetail(newJob), true);
                    if (result == 0)
                    {
                        throw new JobPersistenceException("Could not store job");
                    }
                }
                else
                {
                    _jobDetailRepository.AddJobDetail(new JobDetail(newJob));
                }
            }
        }

        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs,
            bool replace)
        {
            throw new NotImplementedException();
        }

        public bool RemoveJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            throw new NotImplementedException();
        }

        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            using (_lockManager.AcquireLock(Lock.TriggerAccess, InstanceId))
            {
                StoreTrigger(newTrigger, null, replaceExisting, Models.TriggerState.Waiting, false, false);
            }
        }

        private void StoreTrigger(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting, Models.TriggerState state, bool forceState, bool recovering)
        {
            var existingTrigger = _triggerRepository.TriggerExists(newTrigger.Key);

            if (existingTrigger && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            if (!forceState)
            {
            }
        }

        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            throw new NotImplementedException();
        }

        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            throw new NotImplementedException();
        }

        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public bool CalendarExists(string calName)
        {
            throw new NotImplementedException();
        }

        public bool CheckExists(JobKey jobKey)
        {
            throw new NotImplementedException();
        }

        public bool CheckExists(TriggerKey triggerKey)
        {
            throw new NotImplementedException();
        }

        public void ClearAllSchedulingData()
        {
            throw new NotImplementedException();
        }

        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            throw new NotImplementedException();
        }

        public bool RemoveCalendar(string calName)
        {
            throw new NotImplementedException();
        }

        public ICalendar RetrieveCalendar(string calName)
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfJobs()
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfTriggers()
        {
            throw new NotImplementedException();
        }

        public int GetNumberOfCalendars()
        {
            throw new NotImplementedException();
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
    }
}