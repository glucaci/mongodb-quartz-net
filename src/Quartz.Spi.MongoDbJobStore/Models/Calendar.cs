using MongoDB.Bson.Serialization.Attributes;
using Quartz.Simpl;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Models
{
    internal class Calendar
    {
        private static readonly IObjectSerializer ObjectSerializer = new DefaultObjectSerializer();

        public Calendar()
        {
        }

        public Calendar(string calendarName, ICalendar calendar, string instanceName)
        {
            Id = new CalendarId(calendarName, instanceName);
            Content = ObjectSerializer.Serialize(calendar);
        }

        [BsonId]
        public CalendarId Id { get; set; }

        public byte[] Content { get; set; }

        public ICalendar GetCalendar()
        {
            return ObjectSerializer.DeSerialize<ICalendar>(Content);
        }
    }
}