using System.Collections.Generic;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

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

        public IEnumerable<FiredTrigger> GetFiredTriggers(string instanceId)
        {
            return
                Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.InstanceId == instanceId).ToList();
        } 

        public void AddFiredTrigger(FiredTrigger firedTrigger)
        {
            Collection.InsertOne(firedTrigger);
        }

        public void DeleteFiredTrigger(string firedInstanceId)
        {
            Collection.DeleteOne(trigger => trigger.Id == new FiredTriggerId(firedInstanceId, InstanceName));
        }

        public void UpdateFiredTrigger(FiredTrigger firedTrigger)
        {
            Collection.ReplaceOne(trigger => trigger.Id == firedTrigger.Id, firedTrigger);
        }
    }
}