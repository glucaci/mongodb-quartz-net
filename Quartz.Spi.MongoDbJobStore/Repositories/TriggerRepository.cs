using System.Collections.Generic;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("triggers")]
    internal class TriggerRepository : BaseRepository<Trigger>
    {
        public TriggerRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public bool TriggerExists(TriggerKey key)
        {
            return Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).Any();
        }

        public bool TriggersExists(string calendarName)
        {
            return Collection.Find(trigger => trigger.CalendarName == calendarName).Any();
        }

        public Trigger GetTrigger(TriggerKey key)
        {
            return Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).FirstOrDefault();
        }

        public IEnumerable<Trigger> GetTriggers(string calendarName)
        {
            return Collection.Find(FilterBuilder.Where(trigger => trigger.CalendarName == calendarName)).ToList();
        }

        public long GetCount()
        {
            return Collection.Find(trigger => trigger.Id.InstanceName == InstanceName).Count();
        }

        public long GetCount(JobKey jobKey)
        {
            return
                Collection.Find(
                    FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey))
                    .Count();
        }

        public void AddTrigger(Trigger trigger)
        {
            Collection.InsertOne(trigger);
        }

        public void UpdateTrigger(Trigger trigger)
        {
            Collection.ReplaceOne(t => t.Id == trigger.Id, trigger);
        }

        public long DeleteTrigger(TriggerKey key)
        {
            return Collection.DeleteOne(FilterBuilder.Where(trigger => trigger.Id == new TriggerId(key, InstanceName))).DeletedCount;
        }

        public long DeleteTriggers(JobKey jobKey)
        {
            return Collection.DeleteMany(
                FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey)).DeletedCount;
        }
    }
}