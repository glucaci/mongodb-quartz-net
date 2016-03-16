using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal enum TriggerState
    {
        Waiting,
        Acquired,
        Executing,
        Complete,
        Blocked,
        Error,
        Paused,
        PausedBlocked,
        Deleted
    }

    [BsonDiscriminator(RootClass = true)]
    [BsonKnownTypes(typeof (CronTrigger), typeof (SimpleTrigger), typeof (CalendarIntervalTrigger),
        typeof (DailyTimeIntervalTrigger))]
    internal abstract class Trigger
    {
        protected Trigger()
        {
        }

        protected Trigger(ITrigger trigger, TriggerState state, string instanceName)
        {
            Id = new TriggerId()
            {
                InstanceName = instanceName,
                Group = trigger.Key.Group,
                Name = trigger.Key.Name
            };
            JobKey = trigger.JobKey;
            Description = trigger.Description;
            NextFireTime = trigger.GetNextFireTimeUtc();
            PreviousFireTime = trigger.GetPreviousFireTimeUtc();
            State = state;
            StartTime = trigger.StartTimeUtc;
            EndTime = trigger.EndTimeUtc;
            CalendarName = trigger.CalendarName;
            MisfireInstruction = trigger.MisfireInstruction;
            Priority = trigger.Priority;
        }

        [BsonId]
        public TriggerId Id { get; set; }

        public JobKey JobKey { get; set; }

        public string Description { get; set; }

        public DateTimeOffset? NextFireTime { get; set; }

        public DateTimeOffset? PreviousFireTime { get; set; }

        [BsonRepresentation(BsonType.String)]
        public TriggerState State { get; set; }

        public DateTimeOffset StartTime { get; set; }

        public DateTimeOffset? EndTime { get; set; }

        public string CalendarName { get; set; }

        public int MisfireInstruction { get; set; }

        public int Priority { get; set; }

        public string Type { get; set; }

        public JobDataMap JobDataMap { get; set; }
    }

    internal class TriggerId
    {
        public TriggerId() { }

        public TriggerId(TriggerKey triggerKey, string instanceName)
        {
            InstanceName = instanceName;
            Name = triggerKey.Name;
            Group = triggerKey.Group;
        }

        public string InstanceName { get; set; }
        public string Name { get; set; }
        public string Group { get; set; }
    }
}