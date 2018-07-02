using System.Threading.Tasks;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("schedulers")]
    internal class SchedulerRepository : BaseRepository<Scheduler>
    {
        public SchedulerRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public async Task AddScheduler(Scheduler scheduler)
        {
            await Collection.ReplaceOneAsync(sch => sch.Id == scheduler.Id,
                scheduler, new UpdateOptions()
                {
                    IsUpsert = true
                });
        }

        public async Task DeleteScheduler(string id)
        {
            await Collection.DeleteOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName));
        }

        public async Task UpdateState(string id, SchedulerState state)
        {
            await Collection.UpdateOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName),
                UpdateBuilder.Set(sch => sch.State, state));
        }
    }
}