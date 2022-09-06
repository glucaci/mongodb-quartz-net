using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
        public PausedTriggerGroupRepository(IMongoDatabase database, 
            string instanceName, 
            ILogger<PausedTriggerGroupRepository> logger,
            string? collectionPrefix = null)
            : base(database, instanceName, logger, collectionPrefix)
        {
        }

        public async Task<List<string>> GetPausedTriggerGroups()
        {
            var filter = FilterBuilder.Eq(x => x.GroupId.InstanceName, InstanceName);
            return await Collection.Find(filter)
                .Project(group => group.GroupId.Group)
                .ToListAsync()
                .ConfigureAwait(false);
        }

        public async Task<bool> IsTriggerGroupPaused(string group)
        {
            return await Collection.Find(g => g.GroupId == new PausedTriggerGroupId(group, InstanceName)).AnyAsync().ConfigureAwait(false);
        }

        public async Task AddPausedTriggerGroup(string group)
        {
            await Collection.InsertOneAsync(new PausedTriggerGroup()
            {
                GroupId = new PausedTriggerGroupId(group, InstanceName)
            }).ConfigureAwait(false);
        }

        public async Task DeletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher)
        {
            var regex = matcher.ToBsonRegularExpression().ToRegex();
            await Collection.DeleteManyAsync(group => group.GroupId.InstanceName == InstanceName && regex.IsMatch(group.GroupId.Group)).ConfigureAwait(false);
        }

        public async Task DeletePausedTriggerGroup(string groupName)
        {
            await Collection.DeleteOneAsync(group => group.GroupId == new PausedTriggerGroupId(groupName, InstanceName)).ConfigureAwait(false);
        }
    }
}