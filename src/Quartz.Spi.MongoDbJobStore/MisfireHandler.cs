using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quartz.Impl.AdoJobStore;

namespace Quartz.Spi.MongoDbJobStore
{
    internal class MisfireHandler : QuartzThread
    {
        private readonly MongoDbJobStore _jobStore;
        private bool _shutdown;
        private int _numFails;
        private readonly ILogger<MisfireHandler> logger;

        public MisfireHandler(MongoDbJobStore jobStore,
            ILogger<MisfireHandler> logger)
        {
            this.logger = logger;
            _jobStore = jobStore;
            Name = $"QuartzScheduler_{jobStore.InstanceName}-{jobStore.InstanceId}_MisfireHandler";
            IsBackground = true;
        }

        public void Shutdown()
        {
            _shutdown = true;
            Interrupt();
        }

        public override void Run()
        {
            while (!_shutdown)
            {
                var now = DateTime.UtcNow;
                var recoverResult = Manage();
                if (recoverResult.ProcessedMisfiredTriggerCount > 0)
                {
                    _jobStore.SignalSchedulingChangeImmediately(recoverResult.EarliestNewTime);
                }

                if (!_shutdown)
                {
                    var timeToSleep = TimeSpan.FromMilliseconds(50);
                    if (!recoverResult.HasMoreMisfiredTriggers)
                    {
                        timeToSleep = _jobStore.MisfireThreshold - (DateTime.UtcNow - now);
                        if (timeToSleep <= TimeSpan.Zero)
                        {
                            timeToSleep = TimeSpan.FromMilliseconds(50);
                        }

                        if (_numFails > 0)
                        {
                            timeToSleep = _jobStore.DbRetryInterval > timeToSleep
                                ? _jobStore.DbRetryInterval
                                : timeToSleep;
                        }
                    }

                    try
                    {
                        Thread.Sleep(timeToSleep);
                    }
                    catch (ThreadInterruptedException)
                    {
                    }
                }
            }
        }

        private RecoverMisfiredJobsResult Manage()
        {
            try
            {
                logger.LogDebug("Scanning for misfires...");
                var result = _jobStore.DoRecoverMisfires().Result;
                _numFails = 0;
                return result;
            }
            catch (Exception ex)
            {
                if (_numFails%_jobStore.RetryableActionErrorLogThreshold == 0)
                {
                    logger.LogError($"Error handling misfires: {ex.Message}", ex);
                }
                _numFails++;
            }

            return RecoverMisfiredJobsResult.NoOp;
        }
    }
}
