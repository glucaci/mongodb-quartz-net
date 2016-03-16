using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("pausedTriggerGroups")]
    internal class PausedTriggerGroupRepository : BaseRepository<PausedTriggerGroup>
    {
        public PausedTriggerGroupRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public bool IsTriggerGroupPaused(string group)
        {
            return Collection.Find(g => g.Id == new PausedTriggerGroupId(group, InstanceName)).Any();
        }

        public void AddPausedTriggerGroup(string group)
        {
            Collection.InsertOne(new PausedTriggerGroup()
            {
                Id = new PausedTriggerGroupId(group, InstanceName)
            });
        }
    }
}