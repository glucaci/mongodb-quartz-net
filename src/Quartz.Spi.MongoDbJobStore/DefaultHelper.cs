using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.Spi.MongoDbJobStore
{
    public static class QuartzMongoHelper
    {
        public static void SetStoreProperties(NameValueCollection props, string instanceName,string? connectString=null, string? prefx = null)
        {
            props[StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName;
            props[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
            props[StdSchedulerFactory.PropertyJobStoreType] = typeof(MongoDbJobStore).AssemblyQualifiedName;
            props[$"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"] = connectString??"mongodb://localhost/quartz";
            props[$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = prefx ?? string.Empty;
        }
    }
}
