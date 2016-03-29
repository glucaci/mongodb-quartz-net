using System;
using System.Collections.Specialized;
using Quartz.Impl;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected IScheduler CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var properties = new NameValueCollection();
            properties[StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName;
            properties[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
            properties[StdSchedulerFactory.PropertyJobStoreType] = typeof (MongoDbJobStore).AssemblyQualifiedName;
            properties[
                $"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"
                ] = "mongodb://localhost/quartz";
            properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = "prefix";

            var scheduler = new StdSchedulerFactory(properties);
            return scheduler.GetScheduler();
        }
    }
}