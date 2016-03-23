namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class TriggerId : BaseKeyId
    {
        public TriggerId() { }

        public TriggerId(TriggerKey triggerKey, string instanceName)
        {
            InstanceName = instanceName;
            Name = triggerKey.Name;
            Group = triggerKey.Group;
        }

        public TriggerKey GetTriggerKey()
        {
            return new TriggerKey(Name, Group);
        }
    }
}