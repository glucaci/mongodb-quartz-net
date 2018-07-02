using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Extensions;
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

        public async Task<JobDetail> GetJob(JobKey jobKey)
        {
            return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).FirstOrDefaultAsync();
        }

        public async Task<List<JobKey>> GetJobsKeys(GroupMatcher<JobKey> matcher)
        {
            return
                await Collection.Find(FilterBuilder.And(
                    FilterBuilder.Eq(detail => detail.Id.InstanceName, InstanceName),
                    FilterBuilder.Regex(detail => detail.Id.Group, matcher.ToBsonRegularExpression())))
                    .Project(detail => detail.Id.GetJobKey())
                    .ToListAsync();
        }

        public async Task<IEnumerable<string>> GetJobGroupNames()
        {
            return await Collection
                .Distinct(detail => detail.Id.Group, detail => detail.Id.InstanceName == InstanceName)
                .ToListAsync();
        } 

        public async Task AddJob(JobDetail jobDetail)
        {
            await Collection.InsertOneAsync(jobDetail);
        }

        public async Task<long> UpdateJob(JobDetail jobDetail, bool upsert)
        {
            var result = await Collection.ReplaceOneAsync(detail => detail.Id == jobDetail.Id,
                jobDetail,
                new UpdateOptions
                {
                    IsUpsert = upsert
                });
            return result.ModifiedCount;
        }

        public async Task UpdateJobData(JobKey jobKey, JobDataMap jobDataMap)
        {
            await Collection.UpdateOneAsync(detail => detail.Id == new JobDetailId(jobKey, InstanceName),
                UpdateBuilder.Set(detail => detail.JobDataMap, jobDataMap));
        }

        public async Task<long> DeleteJob(JobKey key)
        {
            var result = await Collection.DeleteOneAsync(FilterBuilder.Where(job => job.Id == new JobDetailId(key, InstanceName)));
            return result.DeletedCount;
        }

        public async Task<bool> JobExists(JobKey jobKey)
        {
            return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).AnyAsync();
        }

        public async Task<long> GetCount()
        {
            return await Collection.Find(detail => detail.Id.InstanceName == InstanceName).CountAsync();
        }
    }
}