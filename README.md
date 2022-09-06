MongoDB Job Store for Quartz.NET
================================
Thanks to @chrisdrobison for handing over this project.

## Basic Usage

```cs
var properties = new NameValueCollection();
properties[StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName;
properties[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
properties[StdSchedulerFactory.PropertyJobStoreType] = typeof (MongoDbJobStore).AssemblyQualifiedName;
// I treat the database in the connection string as the one you want to connect to
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"] = "mongodb://localhost/quartz";
// The prefix is optional
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = "prefix";

var scheduler = new StdSchedulerFactory(properties);
return scheduler.GetScheduler();
```

## Nuget

```
Install-Package Quartz.Spi.MongoDbJobStore
```

> mirror from 
https://github.com/Cricle/mongodb-quartz-net (with modify)
and 
https://github.com/glucaci/mongodb-quartz-net