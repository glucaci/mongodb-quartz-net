using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Tests.Jobs;
using Squadron;
using Xunit;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public class MongoDbJobStoreDatabaseTests : IClassFixture<MongoResource>
    {
        private readonly MongoResource _mongoResource;

        public MongoDbJobStoreDatabaseTests(MongoResource mongoResource)
        {
            _mongoResource = mongoResource;
        }

        [Fact]
        public void ConstructorRejectsNullDatabase()
        {
            Action createStore = () => new MongoDbJobStore(null);

            createStore.Should().Throw<ArgumentNullException>().WithParameterName("database");
        }

        [Fact]
        public void DatabaseConstructorPreservesDefaults()
        {
            var store = new MongoDbJobStore(_mongoResource.CreateDatabase());
            var defaultStore = new MongoDbJobStore();

            store.MaxMisfiresToHandleAtATime.Should().Be(defaultStore.MaxMisfiresToHandleAtATime);
            store.RetryableActionErrorLogThreshold.Should().Be(defaultStore.RetryableActionErrorLogThreshold);
            store.DbRetryInterval.Should().Be(defaultStore.DbRetryInterval);
            store.MisfireThreshold.Should().Be(defaultStore.MisfireThreshold);
        }

        [Theory]
        [InlineData(null, null)]
        [InlineData(null, "quartz")]
        [InlineData("", null)]
        [InlineData("", "quartz")]
        public async Task SchedulerUsesInjectedDatabaseWithoutConnectionString(
            string connectionString, string collectionPrefix)
        {
            var database = _mongoResource.CreateDatabase();
            using (var services = CreateServiceProvider(database, connectionString, collectionPrefix))
            {
                var scheduler = await services.GetRequiredService<ISchedulerFactory>().GetScheduler();
                try
                {
                    await StoreAndRetrieveJobAndTrigger(scheduler);
                    await AssertStoredDocuments(database, collectionPrefix);

                    if (collectionPrefix != null)
                    {
                        var collections = await database.ListCollectionNamesAsync();
                        using (collections)
                        {
                            (await collections.ToListAsync()).Should().NotContain("jobs").And.NotContain("triggers");
                        }
                    }
                }
                finally
                {
                    await scheduler.Shutdown();
                }
            }

            await AssertStoredDocuments(database, collectionPrefix);
        }

        [Fact]
        public async Task SchedulerUsesConnectionStringWithoutRegisteredDatabase()
        {
            var database = _mongoResource.CreateDatabase();
            var url = new MongoUrlBuilder(_mongoResource.ConnectionString)
            {
                DatabaseName = database.DatabaseNamespace.DatabaseName
            };

            using (var services = CreateServiceProvider(null, url.ToString(), "quartz"))
            {
                var scheduler = await services.GetRequiredService<ISchedulerFactory>().GetScheduler();
                try
                {
                    await StoreAndRetrieveJobAndTrigger(scheduler);
                    await AssertStoredDocuments(database, "quartz");
                }
                finally
                {
                    await scheduler.Shutdown();
                }
            }
        }

        [Fact]
        public async Task ConfiguredConnectionStringTakesPrecedenceOverRegisteredDatabase()
        {
            var registeredDatabase = _mongoResource.CreateDatabase();
            var configuredDatabase = _mongoResource.CreateDatabase();
            var url = new MongoUrlBuilder(_mongoResource.ConnectionString)
            {
                DatabaseName = configuredDatabase.DatabaseNamespace.DatabaseName
            };

            using (var services = CreateServiceProvider(registeredDatabase, url.ToString(), "quartz"))
            {
                var scheduler = await services.GetRequiredService<ISchedulerFactory>().GetScheduler();
                try
                {
                    await StoreAndRetrieveJobAndTrigger(scheduler);
                    await AssertStoredDocuments(configuredDatabase, "quartz");

                    var collections = await registeredDatabase.ListCollectionNamesAsync();
                    using (collections)
                    {
                        (await collections.ToListAsync()).Should().BeEmpty();
                    }
                }
                finally
                {
                    await scheduler.Shutdown();
                }
            }
        }

        private static ServiceProvider CreateServiceProvider(
            IMongoDatabase database, string connectionString, string collectionPrefix)
        {
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
            if (database != null)
            {
                services.AddSingleton<IMongoDatabase>(database);
            }

            services.AddQuartz(q =>
            {
                q.SchedulerName = $"DATABASE_TEST_{Guid.NewGuid():N}";
                q.UsePersistentStore<MongoDbJobStore>(store => store.UseNewtonsoftJsonSerializer());
                if (connectionString != null)
                {
                    q.SetProperty("quartz.jobStore.connectionString", connectionString);
                }

                if (collectionPrefix != null)
                {
                    q.SetProperty("quartz.jobStore.collectionPrefix", collectionPrefix);
                }
            });

            return services.BuildServiceProvider();
        }

        private static async Task StoreAndRetrieveJobAndTrigger(IScheduler scheduler)
        {
            var job = JobBuilder.Create<SimpleJob>().WithIdentity("job").StoreDurably().Build();
            var trigger = TriggerBuilder.Create().WithIdentity("trigger").ForJob(job)
                .StartAt(DateTimeOffset.UtcNow.AddHours(1)).Build();

            await scheduler.AddJob(job, false);
            await scheduler.ScheduleJob(trigger);

            (await scheduler.GetJobDetail(job.Key)).Key.Should().Be(job.Key);
            (await scheduler.GetTrigger(trigger.Key)).JobKey.Should().Be(job.Key);
        }

        private static async Task AssertStoredDocuments(IMongoDatabase database, string collectionPrefix)
        {
            var prefix = string.IsNullOrEmpty(collectionPrefix) ? "" : $"{collectionPrefix}.";
            (await database.GetCollection<BsonDocument>($"{prefix}jobs")
                .CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(1);
            (await database.GetCollection<BsonDocument>($"{prefix}triggers")
                .CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty)).Should().Be(1);
        }
    }
}
