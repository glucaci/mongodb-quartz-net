using System.Collections.Generic;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("firedTriggers")]
    internal class FiredTriggerRepository : BaseRepository<FiredTrigger>
    {
        public FiredTriggerRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public IEnumerable<FiredTrigger> GetFiredTriggers(JobKey jobKey)
        {
            return
                Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey).ToList();
        }
    }
}