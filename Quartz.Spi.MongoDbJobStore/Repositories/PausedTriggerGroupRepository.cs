using System.Collections.Generic;
using MongoDB.Driver;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Extensions;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("pausedTriggerGroups")]
    internal class PausedTriggerGroupRepository : BaseRepository<PausedTriggerGroup>
    {
        public PausedTriggerGroupRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public IEnumerable<string> GetPausedTriggerGroups()
        {
            return Collection.Find(group => group.Id.InstanceName == InstanceName)
                .Project(group => group.Id.Group)
                .ToList();
        }

        public bool IsTriggerGroupPaused(string group)
        {
            return Collection.Find(g => g.Id == new PausedTriggerGroupId(group, InstanceName)).Any();
        }

        public void AddPausedTriggerGroup(string group)
        {
            Collection.InsertOne(new PausedTriggerGroup()
            {
                Id = new PausedTriggerGroupId(group, InstanceName)
            });
        }

        public void DeletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher)
        {
            var regex = matcher.ToBsonRegularExpression().ToRegex();
            Collection.DeleteMany(group => group.Id.InstanceName == InstanceName && regex.IsMatch(group.Id.Group));
        }

        public void DeletePausedTriggerGroup(string groupName)
        {
            Collection.DeleteOne(group => group.Id == new PausedTriggerGroupId(groupName, InstanceName));
        }
    }
}