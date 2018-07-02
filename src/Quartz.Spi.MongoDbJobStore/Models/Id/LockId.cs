using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class LockId : BaseId
    {
        public LockId() { }

        public LockId(LockType lockType, string instanceName)
        {
            LockType = lockType;
            InstanceName = instanceName;
        }

        [BsonRepresentation(BsonType.String)]
        public LockType LockType { get; set; }

        public override string ToString()
        {
            return $"{LockType}/{InstanceName}";
        }
    }
}