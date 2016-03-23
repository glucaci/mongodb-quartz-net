using System;
using System.Collections.Generic;
using System.Linq;
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

        public bool TriggerExists(TriggerKey key)
        {
            return Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).Any();
        }

        public bool TriggersExists(string calendarName)
        {
            return
                Collection.Find(
                    trigger => trigger.Id.InstanceName == InstanceName && trigger.CalendarName == calendarName).Any();
        }

        public Trigger GetTrigger(TriggerKey key)
        {
            return Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName)).FirstOrDefault();
        }

        public Models.TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            return Collection.Find(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName))
                .Project(trigger => trigger.State)
                .FirstOrDefault();
        }

        public IEnumerable<Trigger> GetTriggers(string calendarName)
        {
            return Collection.Find(FilterBuilder.Where(trigger => trigger.CalendarName == calendarName)).ToList();
        }

        public IEnumerable<Trigger> GetTriggers(JobKey jobKey)
        {
            return
                Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey).ToList();
        }

        public IEnumerable<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            return Collection.Find(FilterBuilder.And(
                FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
                FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression())))
                .Project(trigger => trigger.Id.GetTriggerKey())
                .ToList();
        }

        public IEnumerable<string> GetTriggerGroupNames()
        {
            return Collection.AsQueryable()
                .Where(trigger => trigger.Id.InstanceName == InstanceName)
                .Select(trigger => trigger.Id.Group)
                .Distinct();
        }

        public IEnumerable<string> GetTriggerGroupNames(GroupMatcher<TriggerKey> matcher)
        {
            var regex = matcher.ToBsonRegularExpression().ToRegex();
            return Collection.AsQueryable()
                .Where(trigger => trigger.Id.InstanceName == InstanceName && regex.IsMatch(trigger.Id.Group))
                .Select(trigger => trigger.Id.Group)
                .Distinct();
        }

        public IEnumerable<TriggerKey> GetTriggersToAcquire(DateTimeOffset noLaterThan, DateTimeOffset noEarlierThan,
            int maxCount)
        {
            if (maxCount < 1)
            {
                maxCount = 1;
            }

            var noLaterThanDateTime = noLaterThan.UtcDateTime;
            var noEarlierThanDateTime = noEarlierThan.UtcDateTime;

            return Collection.Find(trigger => trigger.Id.InstanceName == InstanceName &&
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
                .ToList();
        }

        public long GetCount()
        {
            return Collection.Find(trigger => trigger.Id.InstanceName == InstanceName).Count();
        }

        public long GetCount(JobKey jobKey)
        {
            return
                Collection.Find(
                    FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey))
                    .Count();
        }

        public void AddTrigger(Trigger trigger)
        {
            Collection.InsertOne(trigger);
        }

        public void UpdateTrigger(Trigger trigger)
        {
            Collection.ReplaceOne(t => t.Id == trigger.Id, trigger);
        }

        public long UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState state)
        {
            return Collection.UpdateOne(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName),
                UpdateBuilder.Set(trigger => trigger.State, state)).ModifiedCount;
        }

        public long UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState newState, Models.TriggerState oldState)
        {
            return Collection.UpdateOne(
                trigger => trigger.Id == new TriggerId(triggerKey, InstanceName) && trigger.State == oldState,
                UpdateBuilder.Set(trigger => trigger.State, newState)).ModifiedCount;
        }

        public void UpdateTriggersStates(GroupMatcher<TriggerKey> matcher, Models.TriggerState newState,
            params Models.TriggerState[] oldStates)
        {
            Collection.UpdateMany(FilterBuilder.And(
                FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
                FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression()),
                FilterBuilder.In(trigger => trigger.State, oldStates)), 
                UpdateBuilder.Set(trigger => trigger.State, newState));
        }

        public void UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState, Models.TriggerState oldState)
        {
            Collection.UpdateMany(
                trigger =>
                    trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey && trigger.State == oldState,
                UpdateBuilder.Set(trigger => trigger.State, newState));
        }

        public void UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState)
        {
            Collection.UpdateMany(
                trigger =>
                    trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey,
                UpdateBuilder.Set(trigger => trigger.State, newState));
        }

        public long DeleteTrigger(TriggerKey key)
        {
            return
                Collection.DeleteOne(FilterBuilder.Where(trigger => trigger.Id == new TriggerId(key, InstanceName)))
                    .DeletedCount;
        }

        public long DeleteTriggers(JobKey jobKey)
        {
            return Collection.DeleteMany(
                FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey))
                .DeletedCount;
        }
    }
}