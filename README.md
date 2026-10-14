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

## Using a preconfigured database with dependency injection

Register an existing `IMongoDatabase` to reuse its client's settings, including
custom authentication such as OIDC callbacks. With
`Quartz.Extensions.DependencyInjection` and `Quartz.Serialization.Json` installed
(using versions compatible with your Quartz version):

```cs
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Quartz;
using Quartz.Spi.MongoDbJobStore;

// Reuse your application's existing registration if it already provides IMongoDatabase.
services.AddSingleton<IMongoDatabase>(preconfiguredDatabase);

services.AddQuartz(q =>
{
    q.UsePersistentStore<MongoDbJobStore>(store => store.UseNewtonsoftJsonSerializer());
    q.SetProperty("quartz.jobStore.collectionPrefix", "quartz");
});

// Resolve from the application's service provider.
var schedulerFactory = serviceProvider.GetRequiredService<ISchedulerFactory>();
var scheduler = await schedulerFactory.GetScheduler();
```

Quartz's `UsePersistentStore<MongoDbJobStore>()` registers the job store in DI.
When `IMongoDatabase` is registered, DI uses the new
`MongoDbJobStore(IMongoDatabase database)` constructor. The supplied database is
only used when no `quartz.jobStore.connectionString` is configured; an explicitly
configured connection string always takes precedence over the registered
database. When used, the supplied database determines the database name. The
database and its underlying client remain owned by the application; the job
store does not dispose them.

The optional collection prefix works as before: `"quartz"` produces collections
such as `quartz.jobs` and `quartz.triggers`. Without a prefix, collection names
remain `jobs`, `triggers`, etc. To reuse an `IMongoClient`, register
`client.GetDatabase(databaseName)` as `IMongoDatabase`.

Without an `IMongoDatabase` registration, the parameterless constructor and
connection-string configuration continue to work. `StdSchedulerFactory` alone
does not resolve dependencies from DI; use `AddQuartz` and the DI-provided
`ISchedulerFactory` for database injection. Do not configure ADO-specific
persistent-store options (such as `UseProperties`, clustering, or schema
validation) for this MongoDB job store.

## Nuget

```
Install-Package Quartz.Spi.MongoDbJobStore
```