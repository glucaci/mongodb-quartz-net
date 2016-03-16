namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class FiredTriggerId : BaseId
    {
        public FiredTriggerId()
        {
        }

        public FiredTriggerId(string entryId, string instanceName)
        {
            InstanceName = entryId;
            EntryId = entryId;
        }

        public string EntryId { get; set; }
    }
}