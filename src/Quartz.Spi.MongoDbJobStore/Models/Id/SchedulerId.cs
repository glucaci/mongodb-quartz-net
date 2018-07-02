namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class SchedulerId : BaseId
    {
        public SchedulerId() { }

        public SchedulerId(string id, string instanceName)
        {
            Id = id;
            InstanceName = instanceName;
        }

        public string Id { get; set; }

        public override string ToString()
        {
            return $"{Id}/{InstanceName}";
        }
    }
}