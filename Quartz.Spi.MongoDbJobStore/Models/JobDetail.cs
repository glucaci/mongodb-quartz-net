using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class JobDetail
    {
        public JobDetail()
        {
        }

        public JobDetail(IJobDetail jobDetail)
        {
            Key = jobDetail.Key;
            Description = jobDetail.Description;
            JobType = jobDetail.JobType;
            JobDataMap = jobDetail.JobDataMap;
            Durable = jobDetail.Durable;
            PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }

        [BsonId]
        public JobKey Key { get; set; }

        public string Description { get; set; }

        public Type JobType { get; set; }

        public JobDataMap JobDataMap { get; set; }

        public bool Durable { get; set; }

        public bool PersistJobDataAfterExecution { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }
    }
}