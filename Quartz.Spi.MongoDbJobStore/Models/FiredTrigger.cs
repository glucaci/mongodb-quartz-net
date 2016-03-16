using System;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class FiredTrigger
    {
        [BsonId]
        public FiredTriggerId Id { get; set; }

        public TriggerKey TriggerKey { get; set; }

        public JobKey JobKey { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime Fired { get; set; }

        [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
        public DateTime Scheduled { get; set; }

        public int Priority { get; set; }

        public TriggerState State { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }
    }
}