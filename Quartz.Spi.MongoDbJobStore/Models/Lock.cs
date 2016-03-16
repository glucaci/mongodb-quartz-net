using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal enum LockType
    {
        TriggerAccess,
        StateAccess
    }

    internal class Lock
    {
        public const string TriggerAccess = "TRIGGER_ACCESS";
        public const string StateAccess = "STATE_ACCESS";

        [BsonId]
        public LockId Id { get; set; }

        public string InstanceId { get; set; }

        public DateTime AquiredAt { get; set; }
    }

    internal class LockId
    {
        public LockId() { }

        public LockId(LockType lockType, string instanceName)
        {
            LockType = lockType;
            InstanceName = instanceName;
        }

        [BsonRepresentation(BsonType.String)]
        public LockType LockType { get; set; }

        public string InstanceName { get; set; }

        public override string ToString()
        {
            return $"{LockType}/{InstanceName}";
        }
    }
}