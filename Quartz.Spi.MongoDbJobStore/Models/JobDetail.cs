using MongoDB.Bson.Serialization.Attributes;
using Quartz.Simpl;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class JobDetail
    {
        public JobDetail(IJobDetail jobDetail)
        {
            Key = jobDetail.Key;
            Description = jobDetail.Description;
            JobType = $"{jobDetail.JobType.FullName}, {jobDetail.JobType.Assembly.GetName().Name}";

            if (jobDetail.JobDataMap.Count > 0)
            {
                var objSerializer = new DefaultObjectSerializer();
                JobDataMap = objSerializer.Serialize(jobDetail.JobDataMap);
            }

            Durable = jobDetail.Durable;
            PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }

        [BsonId]
        public JobKey Key { get; set; }

        public string Description { get; set; }

        public string JobType { get; set; }

        [BsonElement("JobDataMap")]
        private byte[] JobDataMap { get; set; }

        public bool Durable { get; set; }

        public bool PersistJobDataAfterExecution { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }
    }
}