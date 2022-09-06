using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz.Impl;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var coll = new ServiceCollection();
            coll.AddLogging();
            coll.AddQuartz(config =>
            {
                config.UseMicrosoftDependencyInjectionJobFactory();
                config.UsePersistentStore(opt =>
                {
                    opt.UseJsonSerializer();
                    string conn = null;
                    if (Environment.GetEnvironmentVariable("USE_DOCKER_ENV")!=null)
                    {
                        conn = "mongodb://mongo/quartz";
                    }
                    QuartzMongoHelper.SetStoreProperties(opt.Properties, instanceName,conn);
                });
            });
            var provider = coll.BuildServiceProvider();
            MongoDbJobStore.LoggerFactory = provider.GetRequiredService<ILoggerFactory>();
            var factory = provider.GetRequiredService<ISchedulerFactory>();
            return await factory.GetScheduler();
        }
    }
}