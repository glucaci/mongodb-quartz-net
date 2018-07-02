using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Extensions;
using Quartz.Spi.MongoDbJobStore.Models;
using Quartz.Spi.MongoDbJobStore.Models.Id;

namespace Quartz.Spi.MongoDbJobStore.Repositories
{
    [CollectionName("triggers")]
    internal class TriggerRepository : BaseRepository<Trigger>
    {
        public TriggerRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
            : base(database, instanceName, collectionPrefix)
        {
        }

        public async Task<bool> TriggerExists(TriggerKey key)
        {
            return await Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).AnyAsync();
        }

        public async Task<bool> TriggersExists(string calendarName)
        {
            return
                await Collection.Find(
                    trigger => trigger.Id.InstanceName == InstanceName && trigger.CalendarName == calendarName).AnyAsync();
        }

        public async Task<Trigger> GetTrigger(TriggerKey key)
        {
            return await Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).FirstOrDefaultAsync();
        }

        public async Task<Models.TriggerState> GetTriggerState(TriggerKey triggerKey)
        {
            return await Collection.Find(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName))
                .Project(trigger => trigger.State)
                .FirstOrDefaultAsync();
        }

        public async Task<JobDataMap> GetTriggerJobDataMap(TriggerKey triggerKey)
        {
            return await Collection.Find(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName))
                .Project(trigger => trigger.JobDataMap)
                .FirstOrDefaultAsync();
        }

        public async Task<List<Trigger>> GetTriggers(string calendarName)
        {
            return await Collection.Find(FilterBuilder.Where(trigger => trigger.CalendarName == calendarName)).ToListAsync();
        }

        public async Task<List<Trigger>> GetTriggers(JobKey jobKey)
        {
            return
                await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey).ToListAsync();
        }

        public async Task<List<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            return await Collection.Find(FilterBuilder.And(
                FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
                FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression())))
                .Project(trigger => trigger.Id.GetTriggerKey())
                .ToListAsync();
        }

        public async Task<List<TriggerKey>> GetTriggerKeys(Models.TriggerState state)
        {
            return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.State == state)
                .Project(trigger => trigger.Id.GetTriggerKey())
                .ToListAsync();
        } 

        public async Task<List<string>> GetTriggerGroupNames()
        {
            return await Collection.Distinct(trigger => trigger.Id.Group,
                trigger => trigger.Id.InstanceName == InstanceName)
                .ToListAsync();
        }

        public async Task<List<string>> GetTriggerGroupNames(GroupMatcher<TriggerKey> matcher)
        {
            var regex = matcher.ToBsonRegularExpression().ToRegex();
            return await Collection.Distinct(trigger => trigger.Id.Group,
                    trigger => trigger.Id.InstanceName == InstanceName && regex.IsMatch(trigger.Id.Group))
                .ToListAsync();
        }

        public async Task<List<TriggerKey>> GetTriggersToAcquire(DateTimeOffset noLaterThan, DateTimeOffset noEarlierThan,
            int maxCount)
        {
            if (maxCount < 1)
            {
                maxCount = 1;
            }

            var noLaterThanDateTime = noLaterThan.UtcDateTime;
            var noEarlierThanDateTime = noEarlierThan.UtcDateTime;

            return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName &&
                                              trigger.State == Models.TriggerState.Waiting &&
                                              trigger.NextFireTime <= noLaterThanDateTime &&
                                              (trigger.MisfireInstruction == -1 ||
                                               (trigger.MisfireInstruction != -1 &&
                                                trigger.NextFireTime >= noEarlierThanDateTime)))
                .Sort(SortBuilder.Combine(
                    SortBuilder.Ascending(trigger => trigger.NextFireTime),
                    SortBuilder.Descending(trigger => trigger.Priority)
                    ))
                .Limit(maxCount)
                .Project(trigger => trigger.Id.GetTriggerKey())
                .ToListAsync();
        }

        public async Task<long> GetCount()
        {
            return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName).CountAsync();
        }

        public async Task<long> GetCount(JobKey jobKey)
        {
            return
                await Collection.Find(
                    FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey))
                    .CountAsync();
        }

        public async Task<long> GetMisfireCount(DateTime nextFireTime)
        {
            return
                await Collection.Find(
                    trigger =>
                        trigger.Id.InstanceName == InstanceName &&
                        trigger.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                        trigger.NextFireTime < nextFireTime && trigger.State == Models.TriggerState.Waiting)
                    .CountAsync();
        }

        public async Task AddTrigger(Trigger trigger)
        {
            await Collection.InsertOneAsync(trigger);
        }

        public async Task UpdateTrigger(Trigger trigger)
        {
            await Collection.ReplaceOneAsync(t => t.Id == trigger.Id, trigger);
        }

        public async Task<long> UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState state)
        {
            var result = await Collection.UpdateOneAsync(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName),
                UpdateBuilder.Set(trigger => trigger.State, state));
            return result.ModifiedCount;
        }

        public async Task<long> UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState newState, Models.TriggerState oldState)
        {
            var result = await Collection.UpdateOneAsync(
                trigger => trigger.Id == new TriggerId(triggerKey, InstanceName) && trigger.State == oldState,
                UpdateBuilder.Set(trigger => trigger.State, newState));
            return result.ModifiedCount;
        }

        public async Task<long> UpdateTriggersStates(GroupMatcher<TriggerKey> matcher, Models.TriggerState newState,
            params Models.TriggerState[] oldStates)
        {
            var result = await Collection.UpdateManyAsync(FilterBuilder.And(
                FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
                FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression()),
                FilterBuilder.In(trigger => trigger.State, oldStates)),
                UpdateBuilder.Set(trigger => trigger.State, newState));
            return result.ModifiedCount;
        }

        public async Task<long> UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState,
            params Models.TriggerState[] oldStates)
        {
            var result = await Collection.UpdateManyAsync(
                trigger =>
                    trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey &&
                    oldStates.Contains(trigger.State),
                UpdateBuilder.Set(trigger => trigger.State, newState));
            return result.ModifiedCount;
        }

        public async Task<long> UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState)
        {
            var result = await Collection.UpdateManyAsync(
                trigger =>
                    trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey,
                UpdateBuilder.Set(trigger => trigger.State, newState));
            return result.ModifiedCount;
        }

        public async Task<long> UpdateTriggersStates(Models.TriggerState newState, params Models.TriggerState[] oldStates)
        {
            var result = await Collection.UpdateManyAsync(
                trigger =>
                    trigger.Id.InstanceName == InstanceName && oldStates.Contains(trigger.State),
                UpdateBuilder.Set(trigger => trigger.State, newState));
            return result.ModifiedCount;
        }

        public async Task<long> DeleteTrigger(TriggerKey key)
        {
            var result = 
                await Collection.DeleteOneAsync(FilterBuilder.Where(trigger => trigger.Id == new TriggerId(key, InstanceName)));
            return result.DeletedCount;
        }

        public async Task<long> DeleteTriggers(JobKey jobKey)
        {
            var result = await Collection.DeleteManyAsync(
                FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey));
            return result.DeletedCount;
        }

        /// <summary>
        /// Get the names of all of the triggers in the given state that have
        /// misfired - according to the given timestamp.  No more than count will
        /// be returned.
        /// </summary>
        /// <param name="nextFireTime"></param>
        /// <param name="maxResults"></param>
        /// <param name="results"></param>
        /// <returns></returns>
        public bool HasMisfiredTriggers(DateTime nextFireTime, int maxResults, out List<TriggerKey> results)
        {
            var cursor = Collection.Find(
                trigger => trigger.Id.InstanceName == InstanceName &&
                           trigger.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                           trigger.NextFireTime < nextFireTime &&
                           trigger.State == Models.TriggerState.Waiting)
                .Project(trigger => trigger.Id.GetTriggerKey())
                .Sort(SortBuilder.Combine(
                    SortBuilder.Ascending(trigger => trigger.NextFireTime),
                    SortBuilder.Descending(trigger => trigger.Priority)
                    )).ToCursor();

            results = new List<TriggerKey>();

            var hasReachedLimit = false;
            while (cursor.MoveNext() && !hasReachedLimit)
            {
                foreach (var triggerKey in cursor.Current)
                {
                    if (results.Count == maxResults)
                    {
                        hasReachedLimit = true;
                    }
                    else
                    {
                        results.Add(triggerKey);
                    }
                }
            }
            return hasReachedLimit;
        }
    }
}