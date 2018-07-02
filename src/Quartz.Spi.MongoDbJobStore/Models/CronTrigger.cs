using System;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class CronTrigger : Trigger
    {
        public CronTrigger()
        {
        }

        public CronTrigger(ICronTrigger trigger, TriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            CronExpression = trigger.CronExpressionString;
            TimeZone = trigger.TimeZone.Id;
        }

        public string CronExpression { get; set; }

        public string TimeZone { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new CronTriggerImpl()
            {
                CronExpressionString = CronExpression,
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone)
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}