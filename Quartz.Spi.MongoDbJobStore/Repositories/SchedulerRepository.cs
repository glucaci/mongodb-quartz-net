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

        public void AddScheduler(Scheduler scheduler)
        {
            Collection.ReplaceOne(sch => sch.Id == scheduler.Id,
                scheduler, new UpdateOptions()
                {
                    IsUpsert = true
                });
        }

        public void DeleteScheduler(string id)
        {
            Collection.DeleteOne(sch => sch.Id == new SchedulerId(id, InstanceName));
        }

        public void UpdateState(string id, SchedulerState state)
        {
            Collection.UpdateOne(sch => sch.Id == new SchedulerId(id, InstanceName),
                UpdateBuilder.Set(sch => sch.State, state));
        }
    }
}