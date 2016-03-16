namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class PausedTriggerGroupId : BaseId
    {
        public PausedTriggerGroupId()
        {
        }

        public PausedTriggerGroupId(string group, string instanceName)
        {
            InstanceName = instanceName;
            Group = group;
        }

        public string Group { get; set; }
    }
}