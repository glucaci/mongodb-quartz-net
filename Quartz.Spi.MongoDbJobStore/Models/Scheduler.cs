using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal enum SchedulerState
    {
        Started,
        Running,
        Paused,
        Resumed
    }

    internal class Scheduler
    {
        [BsonId]
        public SchedulerId Id { get; set; }

        [BsonRepresentation(BsonType.String)]
        public SchedulerState State { get; set; }

        public DateTime? LastCheckIn { get; set; }
    }

    internal class SchedulerId
    {
        public SchedulerId() { }

        public SchedulerId(string id, string instanceName)
        {
            Id = id;
            InstanceName = instanceName;
        }

        public string Id { get; set; }
        public string InstanceName { get; set; }

        public override string ToString()
        {
            return $"{Id}/{InstanceName}";
        }
    }
}