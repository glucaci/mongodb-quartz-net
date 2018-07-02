namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class JobDetailId : BaseKeyId
    {
        public JobDetailId()
        {
        }

        public JobDetailId(JobKey jobKey, string instanceName)
        {
            InstanceName = instanceName;
            Name = jobKey.Name;
            Group = jobKey.Group;
        }

        public JobKey GetJobKey()
        {
            return new JobKey(Name, Group);
        }
    }
}