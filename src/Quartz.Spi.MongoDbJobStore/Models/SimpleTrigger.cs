using System;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class SimpleTrigger : Trigger
    {
        public SimpleTrigger()
        {
        }

        public SimpleTrigger(ISimpleTrigger trigger, TriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            RepeatCount = trigger.RepeatCount;
            RepeatInterval = trigger.RepeatInterval;
            TimesTriggered = trigger.TimesTriggered;
        }

        public int RepeatCount { get; set; }

        public TimeSpan RepeatInterval { get; set; }

        public int TimesTriggered { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new SimpleTriggerImpl()
            {
                RepeatCount = RepeatCount,
                RepeatInterval = RepeatInterval,
                TimesTriggered = TimesTriggered
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}