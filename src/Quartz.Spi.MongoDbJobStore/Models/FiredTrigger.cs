using System;
using System.Globalization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Impl.Triggers;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class FiredTrigger
    {
        public FiredTrigger()
        {
        }

        public FiredTrigger(string firedInstanceId, Trigger trigger, JobDetail jobDetail)
        {
            Id = new FiredTriggerId(firedInstanceId, trigger.Id.InstanceName);
            TriggerKey = trigger.Id.GetTriggerKey();
            Fired = DateTime.UtcNow;
            Scheduled = trigger.NextFireTime;
            Priority = trigger.Priority;
            State = trigger.State;

            if (jobDetail != null)
            {
                JobKey = jobDetail.Id.GetJobKey();
                ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
                RequestsRecovery = jobDetail.RequestsRecovery;
            }
        }

        [BsonId]
        public FiredTriggerId Id { get; set; }

        public TriggerKey TriggerKey { get; set; }

        public JobKey JobKey { get; set; }

        public string InstanceId { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime Fired { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? Scheduled { get; set; }

        public int Priority { get; set; }

        [BsonRepresentation(BsonType.String)]
        public TriggerState State { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }

        public IOperableTrigger GetRecoveryTrigger(JobDataMap jobDataMap)
        {
            var firedTime = new DateTimeOffset(Fired);
            var scheduledTime = Scheduled.HasValue ? new DateTimeOffset(Scheduled.Value) : DateTimeOffset.MinValue;
            var recoveryTrigger = new SimpleTriggerImpl($"recover_{InstanceId}_{Guid.NewGuid()}",
                SchedulerConstants.DefaultRecoveryGroup, scheduledTime)
            {
                JobName = JobKey.Name,
                JobGroup = JobKey.Group,
                Priority = Priority,
                MisfireInstruction = MisfireInstruction.IgnoreMisfirePolicy,
                JobDataMap = jobDataMap
            };
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerName, TriggerKey.Name);
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, TriggerKey.Group);
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerFiretime,
                Convert.ToString(firedTime, CultureInfo.InvariantCulture));
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerScheduledFiretime,
                Convert.ToString(scheduledTime, CultureInfo.InvariantCulture));
            return recoveryTrigger;
        }
    }
}