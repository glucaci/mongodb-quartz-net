namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class CronTrigger : Trigger
    {
        public CronTrigger()
        {
        }

        public CronTrigger(ICronTrigger trigger, TriggerState state) : base(trigger, state)
        {
            CronExpression = trigger.CronExpressionString;
            TimeZone = trigger.TimeZone.Id;
        }

        public string CronExpression { get; set; }

        public string TimeZone { get; set; }
    }
}