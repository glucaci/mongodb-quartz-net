using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using Quartz.Spi.MongoDbJobStore.Models;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    internal class PausedTriggerGroupRepository : BaseRepository<PausedTriggerGroup>
    {
        public PausedTriggerGroupRepository(IMongoDatabase database, string collectionPrefix = null) : base(database, collectionPrefix)
        {
        }
    }
}
