using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("jobDetails")]
    internal class JobDetailRepository : BaseRepository<JobDetail>
    {
        public JobDetailRepository(IMongoDatabase database, string collectionPrefix = null)
            : base(database, collectionPrefix)
        {
        }

        public void AddJobDetail(JobDetail jobDetail)
        {
            Collection.InsertOne(jobDetail);
        }

        public long UpdateJobDetail(JobDetail jobDetail, bool upsert)
        {
            var result = Collection.ReplaceOne(detail => detail.Key == jobDetail.Key,
                jobDetail,
                new UpdateOptions
                {
                    IsUpsert = upsert
                });
            return result.ModifiedCount;
        }

        public bool JobExists(JobKey key)
        {
            return Collection.Find(detail => detail.Key == key).Any();
        }
    }
}