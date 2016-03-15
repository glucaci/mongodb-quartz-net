using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class Lock
    {
        public const string TriggerAccess = "TRIGGER_ACCESS";
        public const string StateAccess = "STATE_ACCESS";

        [BsonId]
        public string Name { get; set; }

        public string InstanceId { get; set; }

        public DateTime AquiredAt { get; set; }
    }
}