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
    }
}