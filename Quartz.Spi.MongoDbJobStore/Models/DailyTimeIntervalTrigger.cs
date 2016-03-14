using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class DailyTimeIntervalTrigger : Trigger
    {
        public DailyTimeIntervalTrigger()
        {
        }

        public DailyTimeIntervalTrigger(IDailyTimeIntervalTrigger trigger, TriggerState state) : base(trigger, state)
        {
            RepeatCount = trigger.RepeatCount;
            RepeatIntervalUnit = trigger.RepeatIntervalUnit;
            RepeatInterval = trigger.RepeatInterval;
            StartTimeOfDay = trigger.StartTimeOfDay;
            EndTimeOfDay = trigger.EndTimeOfDay;
            DaysOfWeek = new HashSet<DayOfWeek>(trigger.DaysOfWeek);
            TimesTriggered = trigger.TimesTriggered;
            TimeZone = trigger.TimeZone.Id;
        }

        public int RepeatCount { get; set; }

        [BsonRepresentation(BsonType.String)]
        public IntervalUnit RepeatIntervalUnit { get; set; }

        public int RepeatInterval { get; set; }

        public TimeOfDay StartTimeOfDay { get; set; }

        public TimeOfDay EndTimeOfDay { get; set; }

        public HashSet<DayOfWeek> DaysOfWeek { get; set; }

        public int TimesTriggered { get; set; }

        public string TimeZone { get; set; }
    }
}