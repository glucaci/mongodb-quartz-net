using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;

namespace Quartz.Spi.MongoDbJobStore.Tests.Jobs
{
    public class SimpleJobWithSync : IJob
    {
        public void Execute(IJobExecutionContext context)
        {
            try
            {
                var jobExecTimestamps = (List<DateTime>) context.Scheduler.Context.Get(BaseStoreTests.DateStamps);
                var barrier = (Barrier) context.Scheduler.Context.Get(BaseStoreTests.Barrier);

                jobExecTimestamps.Add(DateTime.UtcNow);

                barrier.SignalAndWait(BaseStoreTests.TestTimeout);
            }
            catch (Exception e)
            {
                Console.Write(e);
                Assert.Fail("Await on barrier was interrupted: " + e);
            }
        }
    }
}