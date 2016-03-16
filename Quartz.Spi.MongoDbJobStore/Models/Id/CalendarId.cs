namespace Quartz.Spi.MongoDbJobStore.Models.Id
{
    internal class CalendarId : BaseId
    {
        public CalendarId()
        {
        }

        public CalendarId(string calendarName, string instanceName)
        {
            InstanceName = instanceName;
            CalendarName = calendarName;
        }

        public string CalendarName { get; set; }
    }
}