using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Quartz.Spi.MongoDbJobStore.Tests.Jobs
{
    public class SimpleJobWithData : IJob
    {
        public string MyTestVar { get; set; }
        public List<string> TestItems { get; set; }

        public static JobDataMap Create(string myVar, List<string> items)
        {
            return new JobDataMap((IDictionary<string,object>)new Dictionary<string, object>()
            {
                { nameof(MyTestVar), myVar },
                { nameof(TestItems), items }
            });
        }
        
        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                if (string.IsNullOrEmpty(MyTestVar))
                {
                    throw new ApplicationException($"{nameof(MyTestVar)} should not be empty.");
                }

                if (TestItems == null || TestItems.Count == 0)
                {
                    throw new ApplicationException($"{nameof(TestItems)} should not be empty.");
                }
                
                var jobExecTimestamps = (List<DateTime>) context.Scheduler.Context.Get(BaseStoreTests.DateStamps);
                var barrier = (Barrier) context.Scheduler.Context.Get(BaseStoreTests.Barrier);

                
                jobExecTimestamps.Add(DateTime.UtcNow);

                barrier.SignalAndWait(BaseStoreTests.TestTimeout);
            }
            catch (Exception e)
            {
                Console.Write(e);
                throw e;
            }

            return Task.FromResult(0);
        }
    }
}
