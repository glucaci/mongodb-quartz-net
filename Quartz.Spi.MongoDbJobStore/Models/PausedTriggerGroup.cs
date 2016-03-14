using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class PausedTriggerGroup
    {
        [BsonId]
        public string Group { get; set; }
    }
}