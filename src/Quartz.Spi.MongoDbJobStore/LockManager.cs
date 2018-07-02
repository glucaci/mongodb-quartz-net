using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Repositories;

namespace Quartz.Spi.MongoDbJobStore
{
    /// <summary>
    /// Implements a simple distributed lock on top of MongoDB. It is not a reentrant lock so you can't 
    /// acquire the lock more than once in the same thread of execution.
    /// </summary>
    internal class LockManager : IDisposable
    {
        private static readonly TimeSpan SleepThreshold = TimeSpan.FromMilliseconds(1000);

        private static readonly ILog Log = LogManager.GetLogger<LockManager>();

        private readonly LockRepository _lockRepository;

        private readonly ConcurrentDictionary<LockType, LockInstance> _pendingLocks =
            new ConcurrentDictionary<LockType, LockInstance>();

        private bool _disposed;

        public LockManager(IMongoDatabase database, string instanceName, string collectionPrefix)
        {
            _lockRepository = new LockRepository(database, instanceName, collectionPrefix);
        }

        public void Dispose()
        {
            EnsureObjectNotDisposed();

            _disposed = true;
            var locks = _pendingLocks.ToArray();
            foreach (var keyValuePair in locks)
            {
                keyValuePair.Value.Dispose();
            }
        }

        public async Task<IDisposable> AcquireLock(LockType lockType, string instanceId)
        {
            while (true)
            {
                EnsureObjectNotDisposed();
                if (await _lockRepository.TryAcquireLock(lockType, instanceId))
                {
                    var lockInstance = new LockInstance(this, lockType, instanceId);
                    AddLock(lockInstance);
                    return lockInstance;
                }
                Thread.Sleep(SleepThreshold);
            }
        }

        private void EnsureObjectNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(LockManager));
            }
        }

        private void AddLock(LockInstance lockInstance)
        {
            if (!_pendingLocks.TryAdd(lockInstance.LockType, lockInstance))
            {
                throw new Exception($"Unable to add lock instance for lock {lockInstance.LockType} on {lockInstance.InstanceId}");
            }
        }

        private void LockReleased(LockInstance lockInstance)
        {
            if (!_pendingLocks.TryRemove(lockInstance.LockType, out _))
            {
                Log.Warn($"Unable to remove pending lock {lockInstance.LockType} on {lockInstance.InstanceId}");
            }
        }

        private class LockInstance : IDisposable
        {
            private readonly LockManager _lockManager;
            private readonly LockRepository _lockRepository;

            private bool _disposed;

            public LockInstance(LockManager lockManager, LockType lockType, string instanceId)
            {
                _lockManager = lockManager;
                LockType = lockType;
                InstanceId = instanceId;
                _lockRepository = lockManager._lockRepository;
            }

            public string InstanceId { get; }

            public LockType LockType { get; }

            public void Dispose()
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(LockInstance),
                        $"This lock {LockType} for {InstanceId} has already been disposed");
                }

                _lockRepository.ReleaseLock(LockType, InstanceId).GetAwaiter().GetResult();
                _lockManager.LockReleased(this);
                _disposed = true;
            }
        }
    }
}