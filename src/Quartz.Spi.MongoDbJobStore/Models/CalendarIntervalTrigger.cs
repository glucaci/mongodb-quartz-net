using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class CalendarIntervalTrigger : Trigger
    {
        public CalendarIntervalTrigger()
        {
        }

        public CalendarIntervalTrigger(ICalendarIntervalTrigger trigger, TriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            RepeatIntervalUnit = trigger.RepeatIntervalUnit;
            RepeatInterval = trigger.RepeatInterval;
            TimesTriggered = trigger.TimesTriggered;
            TimeZone = trigger.TimeZone.Id;
            PreserveHourOfDayAcrossDaylightSavings = trigger.PreserveHourOfDayAcrossDaylightSavings;
            SkipDayIfHourDoesNotExist = trigger.SkipDayIfHourDoesNotExist;
        }

        [BsonRepresentation(BsonType.String)]
        public IntervalUnit RepeatIntervalUnit { get; set; }

        public int RepeatInterval { get; set; }

        public int TimesTriggered { get; set; }

        public string TimeZone { get; set; }

        public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }

        public bool SkipDayIfHourDoesNotExist { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new CalendarIntervalTriggerImpl()
            {
                RepeatIntervalUnit = RepeatIntervalUnit,
                RepeatInterval = RepeatInterval,
                TimesTriggered = TimesTriggered,
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
                PreserveHourOfDayAcrossDaylightSavings = PreserveHourOfDayAcrossDaylightSavings,
                SkipDayIfHourDoesNotExist = SkipDayIfHourDoesNotExist
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}