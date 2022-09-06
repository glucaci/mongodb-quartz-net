using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("locks")]
    internal class LockRepository : BaseRepository<Lock>
    {
        public LockRepository(IMongoDatabase database, 
            string instanceName, 
            ILogger logger,
            string? collectionPrefix = null)
            : base(database, instanceName, logger, collectionPrefix)
        {
        }

        public async Task<bool> TryAcquireLock(LockType lockType, string instanceId)
        {
            var lockId = new LockId(lockType, InstanceName);
            Logger.LogTrace($"Trying to acquire lock {lockId} on {instanceId}");
            try
            {
                await Collection.InsertOneAsync(new Lock
                {
                    Id = lockId,
                    InstanceId = instanceId,
                    AquiredAt = DateTime.Now
                }).ConfigureAwait(false);
                Logger.LogTrace($"Acquired lock {lockId} on {instanceId}");
                return true;
            }
            catch (MongoWriteException)
            {
                Logger.LogTrace($"Failed to acquire lock {lockId} on {instanceId}");
                return false;
            }
        }

        public async Task<bool> ReleaseLock(LockType lockType, string instanceId)
        {
            var lockId = new LockId(lockType, InstanceName);
            Logger.LogTrace($"Releasing lock {lockId} on {instanceId}");
            var result =
                await Collection.DeleteOneAsync(
                    FilterBuilder.Where(@lock => @lock.Id == lockId && @lock.InstanceId == instanceId)).ConfigureAwait(false);
            if (result.DeletedCount > 0)
            {
                Logger.LogTrace($"Released lock {lockId} on {instanceId}");
                return true;
            }
            else
            {
                Logger.LogWarning($"Failed to release lock {lockId} on {instanceId}. You do not own the lock.");
                return false;
            }
        }

        public override async Task EnsureIndex()
        {
            var idx = IndexBuilder.Ascending(@lock => @lock.AquiredAt);
            await Collection.Indexes.CreateOneAsync(new CreateIndexModel<Lock>(idx,
                new CreateIndexOptions() { ExpireAfter = TimeSpan.FromSeconds(30) }))
                .ConfigureAwait(false);
        }
    }
}