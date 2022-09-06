using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class PausedTriggerGroup
    {
        [BsonId]
        public BsonObjectId Id { get; set; }

        public PausedTriggerGroupId GroupId { get; set; }
    }
}