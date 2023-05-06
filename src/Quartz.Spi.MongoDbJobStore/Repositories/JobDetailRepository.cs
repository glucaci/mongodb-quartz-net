﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
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
            return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).FirstOrDefaultAsync().ConfigureAwait(false);
        }

        public async Task<List<JobKey>> GetJobsKeys(GroupMatcher<JobKey> matcher)
        {
            var items = await Collection.Find(FilterBuilder.And(
                    FilterBuilder.Eq(detail => detail.Id.InstanceName, InstanceName),
                    FilterBuilder.Regex(detail => detail.Id.Group, matcher.ToBsonRegularExpression())))
                .Project(detail => new
                {
                    Name = detail.Id.Name,
                    Group = detail.Id.Group
                }) 
                .ToListAsync().ConfigureAwait(false);
            return items.Select(jd => JobKey.Create(jd.Name, jd.Group)).ToList();
        }

        public async Task<IEnumerable<string>> GetJobGroupNames()
        {
            return await Collection
                .Distinct(detail => detail.Id.Group, detail => detail.Id.InstanceName == InstanceName)
                .ToListAsync().ConfigureAwait(false);
        }

        public async Task AddJob(JobDetail jobDetail)
        {
            await Collection.InsertOneAsync(jobDetail).ConfigureAwait(false);
        }

        public async Task<long> UpdateJob(JobDetail jobDetail, bool upsert)
        {
            var result = await Collection.ReplaceOneAsync(detail => detail.Id == jobDetail.Id,
                jobDetail,
                new UpdateOptions
                {
                    IsUpsert = upsert
                }).ConfigureAwait(false);
            return result.ModifiedCount;
        }

        public async Task UpdateJobData(JobKey jobKey, JobDataMap jobDataMap)
        {
            await Collection.UpdateOneAsync(detail => detail.Id == new JobDetailId(jobKey, InstanceName),
                UpdateBuilder.Set(detail => detail.JobDataMap, jobDataMap)).ConfigureAwait(false);
        }

        public async Task<long> DeleteJob(JobKey key)
        {
            var result = await Collection.DeleteOneAsync(FilterBuilder.Where(job => job.Id == new JobDetailId(key, InstanceName))).ConfigureAwait(false);
            return result.DeletedCount;
        }

        public async Task<bool> JobExists(JobKey jobKey)
        {
            return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName)).AnyAsync().ConfigureAwait(false);
        }

        public async Task<long> GetCount()
        {
            return await Collection.Find(detail => detail.Id.InstanceName == InstanceName).CountAsync().ConfigureAwait(false);
        }
    }
}