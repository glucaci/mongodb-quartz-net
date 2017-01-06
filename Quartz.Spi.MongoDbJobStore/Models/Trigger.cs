using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Impl.Triggers;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal enum TriggerState
    {
        None = 0,
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
            NextFireTime = trigger.GetNextFireTimeUtc()?.UtcDateTime;
            PreviousFireTime = trigger.GetPreviousFireTimeUtc()?.UtcDateTime;
            State = state;
            StartTime = trigger.StartTimeUtc.UtcDateTime;
            EndTime = trigger.EndTimeUtc?.UtcDateTime;
            CalendarName = trigger.CalendarName;
            MisfireInstruction = trigger.MisfireInstruction;
            Priority = trigger.Priority;
            JobDataMap = trigger.JobDataMap;
        }

        [BsonId]
        public TriggerId Id { get; set; }

        public JobKey JobKey { get; set; }

        public string Description { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? NextFireTime { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? PreviousFireTime { get; set; }

        [BsonRepresentation(BsonType.String)]
        public TriggerState State { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime StartTime { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime? EndTime { get; set; }

        public string CalendarName { get; set; }

        public int MisfireInstruction { get; set; }

        public int Priority { get; set; }

        public string Type { get; set; }

        public JobDataMap JobDataMap { get; set; }

        public abstract ITrigger GetTrigger();

        protected void FillTrigger(AbstractTrigger trigger)
        {
            trigger.Key = new TriggerKey(Id.Name, Id.Group);
            trigger.JobKey = JobKey;
            trigger.CalendarName = CalendarName;
            trigger.Description = Description;
            trigger.JobDataMap = JobDataMap;
            trigger.MisfireInstruction = MisfireInstruction;
            trigger.EndTimeUtc = EndTime;
            trigger.StartTimeUtc = StartTime;
            trigger.Priority = Priority;
            trigger.SetNextFireTimeUtc(NextFireTime);
            trigger.SetPreviousFireTimeUtc(PreviousFireTime);
        }
    }
}