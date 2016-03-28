using System;
using System.Collections.Specialized;
using Quartz.Impl;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        protected IScheduler CreateScheduler()
        {
            var properties = new NameValueCollection();
            properties[StdSchedulerFactory.PropertySchedulerInstanceName] = "QUARTZ_TEST";
            properties[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
            properties[StdSchedulerFactory.PropertyJobStoreType] = typeof (MongoDbJobStore).AssemblyQualifiedName;
            properties[
                $"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"
                ] = "mongodb://localhost/quartz";

            var scheduler = new StdSchedulerFactory(properties);
            return scheduler.GetScheduler();
        }
    }
}