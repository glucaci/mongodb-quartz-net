using System.Text.Json;
using MongoDB.Bson.Serialization.Attributes;
using Quartz.Simpl;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class Calendar
    {
        public Calendar()
        {
        }

        public Calendar(string calendarName, ICalendar calendar, string instanceName)
        {
            Id = new CalendarId(calendarName, instanceName);
            Content = JsonSerializer.Serialize(calendar);
        }

        [BsonId]
        public CalendarId Id { get; set; }

        public string Content { get; set; }

        public ICalendar GetCalendar()
        {
            return JsonSerializer.Deserialize<ICalendar>(Content);
        }
    }
}