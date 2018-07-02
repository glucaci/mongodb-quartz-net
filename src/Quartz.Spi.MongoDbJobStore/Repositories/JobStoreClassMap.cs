using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Serializers;
using Quartz.Util;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    internal static class JobStoreClassMap
    {
        public static void RegisterClassMaps()
        {
            BsonSerializer.RegisterGenericSerializerDefinition(typeof (ISet<>), typeof (SetSerializer<>));
            BsonSerializer.RegisterSerializer(new JobDataMapSerializer());

            BsonClassMap.RegisterClassMap<Key<JobKey>>(map =>
            {
                map.AutoMap();
                map.MapProperty(key => key.Group);
                map.MapProperty(key => key.Name);
                map.AddKnownType(typeof(JobKey));
            });
            BsonClassMap.RegisterClassMap<Key<TriggerKey>>(map =>
            {
                map.AutoMap();
                map.MapProperty(key => key.Group);
                map.MapProperty(key => key.Name);
                map.AddKnownType(typeof(TriggerKey));
            });
            BsonClassMap.RegisterClassMap<JobKey>(map =>
            {
                map.MapCreator(jobKey => new JobKey(jobKey.Name));
                map.MapCreator(jobKey => new JobKey(jobKey.Name, jobKey.Group));
            });

            BsonClassMap.RegisterClassMap<TriggerKey>(map =>
            {
                map.MapCreator(triggerKey => new TriggerKey(triggerKey.Name));
                map.MapCreator(triggerKey => new TriggerKey(triggerKey.Name, triggerKey.Group));
            });
            BsonClassMap.RegisterClassMap<TimeOfDay>(map =>
            {
                map.AutoMap();
                map.MapProperty(day => day.Hour);
                map.MapProperty(day => day.Minute);
                map.MapProperty(day => day.Second);
                map.MapCreator(day => new TimeOfDay(day.Hour, day.Minute, day.Second));
                map.MapCreator(day => new TimeOfDay(day.Hour, day.Minute));
            });

            BsonClassMap.RegisterClassMap<JobDetail>(map =>
            {
                map.AutoMap();
                map.MapProperty(detail => detail.JobType).SetSerializer(new TypeSerializer());
            });

            BsonClassMap.RegisterClassMap<DailyTimeIntervalTrigger>(map =>
            {
                map.AutoMap();
                var serializer =
                    new EnumerableInterfaceImplementerSerializer
                        <System.Collections.Generic.HashSet<DayOfWeek>, DayOfWeek>(
                        new EnumSerializer<DayOfWeek>(BsonType.String));
                map.MapProperty(trigger => trigger.DaysOfWeek).SetSerializer(serializer);
            });
        }
    }
}