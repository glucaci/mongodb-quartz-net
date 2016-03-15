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

        public LockRepository(IMongoDatabase database, string collectionPrefix = null)
            : base(database, collectionPrefix)
        {
        }

        public bool TryAcquireLock(string lockName, string instanceId)
        {
            Log.Trace($"Trying to acquire lock {lockName} on {instanceId}");
            try
            {
                Collection.InsertOne(new Lock
                {
                    Name = lockName,
                    InstanceId = instanceId,
                    AquiredAt = DateTime.Now
                });
                Log.Trace($"Acquired lock {lockName} on {instanceId}");
                return true;
            }
            catch (MongoWriteException)
            {
                Log.Trace($"Failed to acquire lock {lockName} on {instanceId}");
                return false;
            }
        }

        public bool ReleaseLock(string lockName, string instanceId)
        {
            Log.Trace($"Releasing lock {lockName} on {instanceId}");
            var result =
                Collection.DeleteOne(
                    FilterBuilder.Where(@lock => @lock.Name == lockName && @lock.InstanceId == instanceId));
            if (result.DeletedCount > 0)
            {
                Log.Trace($"Released lock {lockName} on {instanceId}");
                return true;
            }
            else
            {
                Log.Warn($"Failed to release lock {lockName} on {instanceId}. You do not own the lock.");
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