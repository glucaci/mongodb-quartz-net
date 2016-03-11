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

    internal class Trigger
    {
        public Trigger(IOperableTrigger trigger)
        {
        }

        [BsonId]
        public TriggerKey Key { get; set; }

        public JobKey JobKey { get; set; }

        public string Description { get; set; }

        public DateTimeOffset? NextFireTime { get; set; }

        public DateTimeOffset? PreviousFireTime { get; set; }

        [BsonRepresentation(BsonType.String)]
        public TriggerState State { get; set; }

        public DateTimeOffset StartTime { get; set; }

        public DateTimeOffset EndTime { get; set; }

        public string CalendarName { get; set; }

        public int MisfireInstruction { get; set; }

        public int Priority { get; set; }

        [BsonElement("JobDataMap")]
        private byte[] JobDataMap { get; set; }

    }
}