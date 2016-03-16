using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("jobs")]
    internal class JobDetailRepository : BaseRepository<JobDetail>
    {
        public JobDetailRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public JobDetail GetJob(JobKey jobKey)
        {
            return Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).FirstOrDefault();
        }

        public void AddJob(JobDetail jobDetail)
        {
            Collection.InsertOne(jobDetail);
        }

        public long UpdateJob(JobDetail jobDetail, bool upsert)
        {
            var result = Collection.ReplaceOne(detail => detail.Id == jobDetail.Id,
                jobDetail,
                new UpdateOptions
                {
                    IsUpsert = upsert
                });
            return result.ModifiedCount;
        }

        public long DeleteJob(JobKey key)
        {
            var result = Collection.DeleteOne(FilterBuilder.Where(job => job.Id == new JobDetailId(key, InstanceName)));
            return result.DeletedCount;
        }

        public bool JobExists(JobKey jobKey)
        {
            return Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).Any();
        }

        public long GetCount()
        {
            return Collection.Find(detail => detail.Id.InstanceName == InstanceName).Count();
        }
    }
}