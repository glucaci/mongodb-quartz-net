using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Quartz.Impl;
using Squadron;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);
        private readonly MongoResource _mongoResource;

        protected BaseStoreTests(MongoResource mongoResource)
        {
            _mongoResource = mongoResource;
        }

        protected async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var db = _mongoResource.CreateDatabase();

            var baseConn = _mongoResource.ConnectionString;
            var dbName = db.DatabaseNamespace.DatabaseName;
            string finalConnectionString;

            var queryIndex = baseConn.IndexOf('?');
            if (queryIndex >= 0)
            {
                var prefix = baseConn.Substring(0, queryIndex);
                prefix = prefix.TrimEnd('/');
                var queryPart = baseConn.Substring(queryIndex);
                finalConnectionString = $"{prefix}/{dbName}{queryPart}";
            }
            else
            {
                baseConn = baseConn.TrimEnd('/');
                finalConnectionString = $"{baseConn}/{dbName}";
            }

            var properties = new NameValueCollection
            {
                ["quartz.serializer.type"] = "json",
                [StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
                [StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
                [StdSchedulerFactory.PropertyJobStoreType] = typeof(MongoDbJobStore).AssemblyQualifiedName,
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"]
                    = finalConnectionString,
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = "prefix"
            };

            var scheduler = new StdSchedulerFactory(properties);
            return await scheduler.GetScheduler();
        }
    }
}