using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("calendars")]
    internal class CalendarRepository : BaseRepository<Calendar>
    {
        public CalendarRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public bool CalendarExists(string calendarName)
        {
            return
                Collection.Find(
                    FilterBuilder.Where(calendar => calendar.Id == new CalendarId(calendarName, InstanceName))).Any();
        }

        public Calendar GetCalendar(string calendarName)
        {
            return
                Collection.Find(calendar => calendar.Id == new CalendarId(calendarName, InstanceName)).FirstOrDefault();
        }

        public IEnumerable<string> GetCalendarNames()
        {
            return Collection.AsQueryable()
                .Where(calendar => calendar.Id.InstanceName == InstanceName)
                .Select(calendar => calendar.Id.CalendarName)
                .Distinct();
        } 

        public long GetCount()
        {
            return Collection.Find(calendar => calendar.Id.InstanceName == InstanceName).Count();
        }

        public void AddCalendar(Calendar calendar)
        {
            Collection.InsertOne(calendar);
        }

        public long UpdateCalendar(Calendar calendar)
        {
            return Collection.ReplaceOne(cal => cal.Id == calendar.Id, calendar).MatchedCount;
        }

        public long DeleteCalendar(string calendarName)
        {
            return
                Collection.DeleteOne(calendar => calendar.Id == new CalendarId(calendarName, InstanceName))
                    .DeletedCount;
        }
    }
}