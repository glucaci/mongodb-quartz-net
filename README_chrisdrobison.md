[![Build status](https://ci.appveyor.com/api/projects/status/e7coq0xrmv3imbtt?svg=true)](https://ci.appveyor.com/project/chrisdrobison/mongodb-quartz-net)

## Note: This project isn't maintained. Any volunteers to update it and maintain it are welcome.

MongoDB Job Store for Quartz.NET
================================

This project was born out of the need to support the MongoDB C# 2.0 driver. I used to use [https://github.com/Nanoko/quartznet-mongodb](https://github.com/Nanoko/quartznet-mongodb) and that worked for a while, 
but found there were issues with job recovery every once and a while and it enforced driver conventions we did not want. The project has also not been updated in quite some time. Before starting this project,
I thought it might be good enough to upgrade the driver in that project and fix all the database calls, but decided against that for a number of reasons:

* The project works directly with the BsonDocument object, which is error prone if you don't have some simple way of tracking the field names you use (that project has lots of magic strings)
* Documentation on custom job stores is lacking and the way to implement new ones is to follow the reference ADO.NET implementations. I don't feel like that project followed those implementations very well.
* It specifies a driver convention which affects the project using the library (at least, I had errors trying to use it). It's my opinion the conventions should be left up to program using the job store.
* The project is mapping private fields for triggers thus making the implementation pretty brittle

## Goals ##

I wanted to take this implementation in a different direction to help ease future improvements. Here is what I've done differently:

* Followed the implementation of the Quartz.NET JobStoreSupport class very closely. The logic should be almost identical except where database calls are concerned. Should make future updates easier to implement.
* Implemented a type safe MongoDB repository pattern. Everything is stored using a real class. I've implemented a factory pattern to handle the different trigger types. I've avoided using BsonDocument completely.
* Implemented a simple distributed lock on top of Mongo to help with future cluster support based on ideas shared [here](https://speakerdeck.com/raindev/distributed-locking-with-mongodb).
* Imported the job store unit tests from Quartz.NET and got them passing.

## Basic Usage##

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

## Nuget ##

```
Install-Package Quartz.Spi.MongoDbJobStore
```

