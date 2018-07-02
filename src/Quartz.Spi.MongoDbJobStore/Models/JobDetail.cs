using System;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Impl;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class JobDetail
    {
        public JobDetail()
        {
        }

        public JobDetail(IJobDetail jobDetail, string instanceName)
        {
            Id = new JobDetailId(jobDetail.Key, instanceName);
            Description = jobDetail.Description;
            JobType = jobDetail.JobType;
            JobDataMap = jobDetail.JobDataMap;
            Durable = jobDetail.Durable;
            PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }

        [BsonId]
        public JobDetailId Id { get; set; }

        public string Description { get; set; }

        public Type JobType { get; set; }

        public JobDataMap JobDataMap { get; set; }

        public bool Durable { get; set; }

        public bool PersistJobDataAfterExecution { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }

        public IJobDetail GetJobDetail()
        {
            // The missing properties are figured out at runtime from the job type attributes
            return new JobDetailImpl()
            {
                Key = new JobKey(Id.Name, Id.Group),
                Description = Description,
                JobType = JobType,
                JobDataMap = JobDataMap,
                Durable = Durable,
                RequestsRecovery = RequestsRecovery
            };
        }
    }
}