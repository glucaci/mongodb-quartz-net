using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("schedulers")]
    internal class SchedulerRepository : BaseRepository<Scheduler>
    {
        public SchedulerRepository(IMongoDatabase database, string collectionPrefix = null)
            : base(database, collectionPrefix)
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

        public void DeleteScheduler(string schedulerId)
        {
            Collection.DeleteOne(sch => sch.Id == schedulerId);
        }

        public void UpdateState(string schedulerId, SchedulerState state)
        {
            Collection.UpdateOne(sch => sch.Id == schedulerId,
                UpdateBuilder.Set(sch => sch.State, state));
        }
    }
}