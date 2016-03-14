using System;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore
{
    internal static class TriggerFactory
    {
        public static Trigger CreateTrigger(ITrigger trigger, Models.TriggerState state)
        {
            if (trigger is ICronTrigger)
            {
                return new CronTrigger((ICronTrigger) trigger, state);
            }
            if (trigger is ISimpleTrigger)
            {
                return new SimpleTrigger((ISimpleTrigger) trigger, state);
            }
            if (trigger is ICalendarIntervalTrigger)
            {
                return new CalendarIntervalTrigger((ICalendarIntervalTrigger) trigger, state);
            }
            if (trigger is IDailyTimeIntervalTrigger)
            {
                return new DailyTimeIntervalTrigger((IDailyTimeIntervalTrigger) trigger, state);
            }

            throw new NotSupportedException($"Trigger of type {trigger.GetType().FullName} is not supported");
        }
    }
}