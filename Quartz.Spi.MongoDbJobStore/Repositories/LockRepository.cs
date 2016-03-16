using System;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("locks")]
    internal class LockRepository : BaseRepository<Lock>
    {
        private static readonly ILog Log = LogManager.GetLogger<LockRepository>();

        public LockRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public bool TryAcquireLock(LockType lockType, string instanceId)
        {
            var lockId = new LockId(lockType, InstanceName);
            Log.Trace($"Trying to acquire lock {lockId} on {instanceId}");
            try
            {
                Collection.InsertOne(new Lock
                {
                    Id = lockId,
                    InstanceId = instanceId,
                    AquiredAt = DateTime.Now
                });
                Log.Trace($"Acquired lock {lockId} on {instanceId}");
                return true;
            }
            catch (MongoWriteException)
            {
                Log.Trace($"Failed to acquire lock {lockId} on {instanceId}");
                return false;
            }
        }

        public bool ReleaseLock(LockType lockType, string instanceId)
        {
            var lockId = new LockId(lockType, InstanceName);
            Log.Trace($"Releasing lock {lockId} on {instanceId}");
            var result =
                Collection.DeleteOne(
                    FilterBuilder.Where(@lock => @lock.Id == lockId && @lock.InstanceId == instanceId));
            if (result.DeletedCount > 0)
            {
                Log.Trace($"Released lock {lockId} on {instanceId}");
                return true;
            }
            else
            {
                Log.Warn($"Failed to release lock {lockId} on {instanceId}. You do not own the lock.");
                return false;
            }
        }

        public override void EnsureIndex()
        {
            Collection.Indexes.CreateOne(IndexBuilder.Ascending(@lock => @lock.AquiredAt),
                new CreateIndexOptions() {ExpireAfter = TimeSpan.FromSeconds(30)});
        }
    }
}