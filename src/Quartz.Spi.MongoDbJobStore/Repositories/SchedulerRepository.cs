using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("schedulers")]
    internal class SchedulerRepository : BaseRepository<Scheduler>
    {
        public SchedulerRepository(IMongoDatabase database, 
            string instanceName,
            ILogger<SchedulerRepository> logger,
            string? collectionPrefix = null)
            : base(database, instanceName, logger, collectionPrefix)
        {
        }

        public async Task AddScheduler(Scheduler scheduler)
        {
            await Collection.ReplaceOneAsync(sch => sch.Id == scheduler.Id,
                scheduler, new ReplaceOptions
                {
                    IsUpsert = true
                }).ConfigureAwait(false);
        }

        public async Task DeleteScheduler(string id)
        {
            await Collection.DeleteOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName)).ConfigureAwait(false);
        }

        public async Task UpdateState(string id, SchedulerState state)
        {
            await Collection.UpdateOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName),
                UpdateBuilder.Set(sch => sch.State, state)).ConfigureAwait(false);
        }
    }
}