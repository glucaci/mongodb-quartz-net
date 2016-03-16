using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    internal class TriggerRepository : BaseRepository<Trigger>
    {
        public TriggerRepository(IMongoDatabase database, string collectionPrefix = null)
            : base(database, collectionPrefix)
        {
        }

        public bool TriggerExists(TriggerId triggerId)
        {
            return Collection.Find(trigger => trigger.Id == triggerId).Any();
        }
    }
}