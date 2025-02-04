using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Quartz.Impl;
using Squadron;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        private readonly MongoResource _mongoResource;
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected BaseStoreTests(MongoResource mongoResource)
        {
            _mongoResource = mongoResource;
        }

        protected async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var db = _mongoResource.CreateDatabase();

            var properties = new NameValueCollection
            {
                ["quartz.serializer.type"] = "json",
                [StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
                [StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
                [StdSchedulerFactory.PropertyJobStoreType] = typeof(MongoDbJobStore).AssemblyQualifiedName,
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"]
                    = $"{_mongoResource.ConnectionString}/{db.DatabaseNamespace.DatabaseName}",
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = "prefix"
            };

            var scheduler = new StdSchedulerFactory(properties);
            return await scheduler.GetScheduler();
        }
    }
}