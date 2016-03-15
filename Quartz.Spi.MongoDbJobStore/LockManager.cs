using System;
using System.Collections.Concurrent;
using System.Threading;
using Common.Logging;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Repositories;

namespace Quartz.Spi.MongoDbJobStore
{
    public class LockManager : IDisposable
    {
        private static readonly TimeSpan SleepThreshold = TimeSpan.FromMilliseconds(1000);

        private static readonly ILog Log = LogManager.GetLogger<LockManager>();

        private readonly LockRepository _lockRepository;

        private readonly ConcurrentDictionary<string, LockInstance> _pendingLocks =
            new ConcurrentDictionary<string, LockInstance>();

        private bool _disposed;

        public LockManager(IMongoDatabase database, string instanceId)
        {
            _lockRepository = new LockRepository(database, instanceId);
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

        public IDisposable AcquireLock(string lockName, string instanceId)
        {
            while (true)
            {
                EnsureObjectNotDisposed();
                if (_lockRepository.TryAcquireLock(lockName, instanceId))
                {
                    var lockInstance = new LockInstance(this, lockName, instanceId);
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
            if (!_pendingLocks.TryAdd(lockInstance.LockName, lockInstance))
            {
                throw new Exception($"Unable to add lock instance for lock {lockInstance.LockName} on {lockInstance.InstanceId}");
            }
        }

        private void LockReleased(LockInstance lockInstance)
        {
            LockInstance releasedLock;
            if (!_pendingLocks.TryRemove(lockInstance.LockName, out releasedLock))
            {
                Log.Warn($"Unable to remove pending lock {lockInstance.LockName} on {lockInstance.InstanceId}");
            }
        }

        private class LockInstance : IDisposable
        {
            private readonly LockManager _lockManager;
            private readonly LockRepository _lockRepository;

            private bool _disposed;

            public LockInstance(LockManager lockManager, string lockName, string instanceId)
            {
                _lockManager = lockManager;
                LockName = lockName;
                InstanceId = instanceId;
                _lockRepository = lockManager._lockRepository;
            }

            public string InstanceId { get; }

            public string LockName { get; }

            public void Dispose()
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(LockInstance),
                        $"This lock {LockName} for {InstanceId} has already been disposed");
                }

                _lockRepository.ReleaseLock(LockName, InstanceId);
                _lockManager.LockReleased(this);
                _disposed = true;
            }
        }
    }
}