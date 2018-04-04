using System;
using System.Threading;
using NUnit.Framework;
using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
//using Quartz.Job;
using Quartz.Spi;
using Quartz.Util;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Quartz.Tests.Integration.Impl
{
    public class SmokeTestPerformer
    {
        public void Test(IScheduler scheduler, bool clearJobs, bool scheduleJobs)
        {
            try
            {
                if (clearJobs)
                {
                    scheduler.Clear().Wait();
                }

                if (scheduleJobs)
                {
                    ICalendar cronCalendar = new CronCalendar("0/5 * * * * ?");
                    ICalendar holidayCalendar = new HolidayCalendar();

                    // QRTZNET-86
                    var t = scheduler.GetTrigger(new TriggerKey("NonExistingTrigger", "NonExistingGroup")).Result;
                    Assert.IsNull(t);

                    var cal = new AnnualCalendar();
                    scheduler.AddCalendar("annualCalendar", cal, false, true).Wait();

                    IOperableTrigger calendarsTrigger = new SimpleTriggerImpl("calendarsTrigger", "test", 20,
                        TimeSpan.FromMilliseconds(5));
                    calendarsTrigger.CalendarName = "annualCalendar";

                    var jd = new JobDetailImpl("testJob", "test", typeof (NoOpJob));
                    scheduler.ScheduleJob(jd, calendarsTrigger).Wait();

                    // QRTZNET-93
                    scheduler.AddCalendar("annualCalendar", cal, true, true).Wait();

                    scheduler.AddCalendar("baseCalendar", new BaseCalendar(), false, true).Wait();
                    scheduler.AddCalendar("cronCalendar", cronCalendar, false, true).Wait();
                    scheduler.AddCalendar("dailyCalendar",
                        new DailyCalendar(DateTime.Now.Date, DateTime.Now.AddMinutes(1)), false, true).Wait();
                    scheduler.AddCalendar("holidayCalendar", holidayCalendar, false, true).Wait();
                    scheduler.AddCalendar("monthlyCalendar", new MonthlyCalendar(), false, true).Wait();
                    scheduler.AddCalendar("weeklyCalendar", new WeeklyCalendar(), false, true).Wait();

                    scheduler.AddCalendar("cronCalendar", cronCalendar, true, true).Wait();
                    scheduler.AddCalendar("holidayCalendar", holidayCalendar, true, true).Wait();

                    Assert.IsNotNull(scheduler.GetCalendar("annualCalendar").Result);

                    var lonelyJob = new JobDetailImpl("lonelyJob", "lonelyGroup", typeof (SimpleRecoveryJob));
                    lonelyJob.Durable = true;
                    lonelyJob.RequestsRecovery = true;
                    scheduler.AddJob(lonelyJob, false).Wait();
                    scheduler.AddJob(lonelyJob, true).Wait();

                    var schedId = scheduler.SchedulerInstanceId;

                    var count = 1;

                    var job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));

                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    IOperableTrigger trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20,
                        TimeSpan.FromSeconds(5));
                    trigger.JobDataMap.Add("key", "value");
                    trigger.EndTimeUtc = DateTime.UtcNow.AddYears(10);

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    scheduler.ScheduleJob(job, trigger).Wait();

                    // check that trigger was stored
                    var persisted = scheduler.GetTrigger(new TriggerKey("trig_" + count, schedId)).Result;
                    Assert.IsNotNull(persisted);
                    Assert.IsTrue(persisted is SimpleTriggerImpl);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(5));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(2000L);
                    scheduler.ScheduleJob(job, trigger).Wait();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryStatefulJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(3));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    scheduler.ScheduleJob(job, trigger).Wait();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(4));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    scheduler.ScheduleJob(job, trigger).Wait();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromMilliseconds(4500));
                    scheduler.ScheduleJob(job, trigger).Wait();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    IOperableTrigger ct = new CronTriggerImpl("cron_trig_" + count, schedId, "0/10 * * * * ?");
                    ct.JobDataMap.Add("key", "value");
                    ct.StartTimeUtc = DateTime.Now.AddMilliseconds(1000);

                    scheduler.ScheduleJob(job, ct).Wait();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    var nt = new DailyTimeIntervalTriggerImpl("nth_trig_" + count, schedId, new TimeOfDay(1, 1, 1),
                        new TimeOfDay(23, 30, 0), IntervalUnit.Hour, 1);
                    nt.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);

                    scheduler.ScheduleJob(job, nt).Wait();

                    var nt2 = new DailyTimeIntervalTriggerImpl();
                    nt2.Key = new TriggerKey("nth_trig2_" + count, schedId);
                    nt2.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);
                    nt2.JobKey = job.Key;
                    scheduler.ScheduleJob(nt2).Wait();

                    // GitHub issue #92
                    scheduler.GetTrigger(nt2.Key).Wait();

                    // GitHub issue #98
                    nt2.StartTimeOfDay = new TimeOfDay(1, 2, 3);
                    nt2.EndTimeOfDay = new TimeOfDay(2, 3, 4);

                    scheduler.UnscheduleJob(nt2.Key).Wait();
                    scheduler.ScheduleJob(nt2).Wait();

                    var triggerFromDb = (IDailyTimeIntervalTrigger) scheduler.GetTrigger(nt2.Key).Result;
                    Assert.That(triggerFromDb.StartTimeOfDay.Hour, Is.EqualTo(1));
                    Assert.That(triggerFromDb.StartTimeOfDay.Minute, Is.EqualTo(2));
                    Assert.That(triggerFromDb.StartTimeOfDay.Second, Is.EqualTo(3));

                    Assert.That(triggerFromDb.EndTimeOfDay.Hour, Is.EqualTo(2));
                    Assert.That(triggerFromDb.EndTimeOfDay.Minute, Is.EqualTo(3));
                    Assert.That(triggerFromDb.EndTimeOfDay.Second, Is.EqualTo(4));

                    job.RequestsRecovery = true;
                    var intervalTrigger = new CalendarIntervalTriggerImpl(
                        "calint_trig_" + count,
                        schedId,
                        DateTime.UtcNow.AddMilliseconds(300),
                        DateTime.UtcNow.AddMinutes(1),
                        IntervalUnit.Second,
                        8);
                    intervalTrigger.JobKey = job.Key;

                    scheduler.ScheduleJob(intervalTrigger).Wait();

                    // bulk operations
                    IJobDetail detail = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    ITrigger simple = new SimpleTriggerImpl("trig_" + count, schedId, 20,
                        TimeSpan.FromMilliseconds(4500));
                    var triggers = (IReadOnlyCollection<ITrigger>)new HashSet<ITrigger> {simple};
                    var info = (IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>>)new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
                    {
                        [detail] = triggers
                    };

                    scheduler.ScheduleJobs(info, true).Wait();

                    Assert.IsTrue(scheduler.CheckExists(detail.Key).Result);
                    Assert.IsTrue(scheduler.CheckExists(simple.Key).Result);

                    // QRTZNET-243
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("a").DeepClone()).Wait();
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("a").DeepClone()).Wait();
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("a").DeepClone()).Wait();
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("a").DeepClone()).Wait();

                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("a").DeepClone()).Wait();
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("a").DeepClone()).Wait();
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("a").DeepClone()).Wait();
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("a").DeepClone()).Wait();

                    scheduler.Start().Wait();

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    scheduler.PauseAll().Wait();

                    scheduler.ResumeAll().Wait();

                    scheduler.PauseJob(new JobKey("job_1", schedId)).Wait();

                    scheduler.ResumeJob(new JobKey("job_1", schedId)).Wait();

                    scheduler.PauseJobs(GroupMatcher<JobKey>.GroupEquals(schedId)).Wait();

                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    scheduler.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(schedId)).Wait();

                    scheduler.PauseTrigger(new TriggerKey("trig_2", schedId)).Wait();
                    scheduler.ResumeTrigger(new TriggerKey("trig_2", schedId)).Wait();

                    scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId)).Wait();

                    Assert.AreEqual(1, scheduler.GetPausedTriggerGroups().Result.Count);

                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId)).Wait();

                    Assert.IsNotNull(scheduler.GetTrigger(new TriggerKey("trig_2", schedId)).Result);
                    Assert.IsNotNull(scheduler.GetJobDetail(new JobKey("job_1", schedId)).Result);
                    Assert.IsNotNull(scheduler.GetMetaData().Result);
                    Assert.IsNotNull(scheduler.GetCalendar("weeklyCalendar").Result);

                    var genericjobKey = new JobKey("genericJob", "genericGroup");
                    var genericJob = JobBuilder.Create<GenericJobType<string>>()
                        .WithIdentity(genericjobKey)
                        .StoreDurably()
                        .Build();

                    scheduler.AddJob(genericJob, false).Wait();

                    genericJob = scheduler.GetJobDetail(genericjobKey).Result;
                    Assert.That(genericJob, Is.Not.Null);
                    scheduler.TriggerJob(genericjobKey).Wait();

                    Thread.Sleep(TimeSpan.FromSeconds(20));

                    Assert.That(GenericJobType<string>.TriggeredCount, Is.EqualTo(1));
                    scheduler.Standby().Wait();

                    CollectionAssert.IsNotEmpty(scheduler.GetCalendarNames().Result);
                    CollectionAssert.IsNotEmpty(scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(schedId)).Result);

                    CollectionAssert.IsNotEmpty(scheduler.GetTriggersOfJob(new JobKey("job_2", schedId)).Result);
                    Assert.IsNotNull(scheduler.GetJobDetail(new JobKey("job_2", schedId)).Result);

                    scheduler.DeleteCalendar("cronCalendar").Wait();
                    scheduler.DeleteCalendar("holidayCalendar").Wait();
                    scheduler.DeleteJob(new JobKey("lonelyJob", "lonelyGroup")).Wait();
                    scheduler.DeleteJob(job.Key).Wait();

                    scheduler.GetJobGroupNames().Wait();
                    scheduler.GetCalendarNames().Wait();
                    scheduler.GetTriggerGroupNames().Wait();

                    TestMatchers(scheduler);
                }
            }
            finally
            {
                scheduler.Shutdown(false).Wait();
            }
        }

        private void TestMatchers(IScheduler scheduler)
        {
            scheduler.Clear().Wait();

            var job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "aaabbbccc").StoreDurably().Build();
            scheduler.AddJob(job, true).Wait();
            var schedule = SimpleScheduleBuilder.Create();
            var trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "aaabbbccc").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger).Wait();

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "xxxyyyzzz").StoreDurably().Build();
            scheduler.AddJob(job, true).Wait();
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger).Wait();

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job2", "xxxyyyzzz").StoreDurably().Build();
            scheduler.AddJob(job, true).Wait();
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig2", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger).Wait();

            var jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup()).Result;
            Assert.That(jkeys.Count, Is.EqualTo(3), "Wrong number of jobs found by anything matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("xxxyyyzzz")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by equals matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("aaabbbccc")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by equals matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("aa")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by starts with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("xx")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by starts with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("cc")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by ends with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("zzz")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by ends with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("bc")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by contains with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("yz")).Result;
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by contains with matcher");

            var tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.AnyGroup()).Result;
            Assert.That(tkeys.Count, Is.EqualTo(3), "Wrong number of triggers found by anything matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("xxxyyyzzz")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by equals matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("aaabbbccc")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by equals matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("aa")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by starts with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("xx")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by starts with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("cc")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by ends with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("zzz")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by ends with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("bc")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by contains with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("yz")).Result;
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by contains with matcher");
        }
    }

    public class NoOpJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            return Task.FromResult(0);
        }
    }

    public class GenericJobType<T> : IJob
    {
        public static int TriggeredCount { get; private set; }

        public Task Execute(IJobExecutionContext context)
        {
            TriggeredCount++;
            return Task.FromResult(0);
        }
    }

    public class SimpleRecoveryJob : IJob
    {
        private const string Count = "count";

        /// <summary>
        ///     Called by the <see cref="IScheduler" /> when a
        ///     <see cref="ITrigger" /> fires that is associated with
        ///     the <see cref="IJob" />.
        /// </summary>
        public virtual Task Execute(IJobExecutionContext context)
        {
            // delay for ten seconds
            try
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
            }
            catch (ThreadInterruptedException)
            {
            }

            var data = context.JobDetail.JobDataMap;
            int count;
            if (data.ContainsKey(Count))
            {
                count = data.GetInt(Count);
            }
            else
            {
                count = 0;
            }
            count++;
            data.Put(Count, count);
            return Task.FromResult(0);
        }
    }

    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class SimpleRecoveryStatefulJob : SimpleRecoveryJob
    {
    }
}