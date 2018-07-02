using System.Collections.Generic;
using System.Threading.Tasks;
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

        public async Task<List<FiredTrigger>> GetFiredTriggers(JobKey jobKey)
        {
            return
                await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey).ToListAsync();
        }

        public async Task<List<FiredTrigger>> GetFiredTriggers(string instanceId)
        {
            return
                await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.InstanceId == instanceId)
                    .ToListAsync();
        }

        public async Task<List<FiredTrigger>> GetRecoverableFiredTriggers(string instanceId)
        {
            return
                await Collection.Find(
                    trigger =>
                        trigger.Id.InstanceName == InstanceName && trigger.InstanceId == instanceId &&
                        trigger.RequestsRecovery).ToListAsync();
        }

        public async Task AddFiredTrigger(FiredTrigger firedTrigger)
        {
            await Collection.InsertOneAsync(firedTrigger);
        }

        public async Task DeleteFiredTrigger(string firedInstanceId)
        {
            await Collection.DeleteOneAsync(trigger => trigger.Id == new FiredTriggerId(firedInstanceId, InstanceName));
        }

        public async Task<long> DeleteFiredTriggersByInstanceId(string instanceId)
        {
            var result =
                await Collection.DeleteManyAsync(
                    trigger => trigger.Id.InstanceName == InstanceName && trigger.InstanceId == instanceId);
            return result.DeletedCount;
        }

        public async Task UpdateFiredTrigger(FiredTrigger firedTrigger)
        {
            await Collection.ReplaceOneAsync(trigger => trigger.Id == firedTrigger.Id, firedTrigger);
        }
    }
}