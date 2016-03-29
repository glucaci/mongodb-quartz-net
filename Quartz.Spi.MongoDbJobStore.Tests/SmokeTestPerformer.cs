using System;
using System.Threading;
using NUnit.Framework;
using Quartz.Collection;
using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Job;
using Quartz.Spi;
using Quartz.Util;

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
                    scheduler.Clear();
                }

                if (scheduleJobs)
                {
                    ICalendar cronCalendar = new CronCalendar("0/5 * * * * ?");
                    ICalendar holidayCalendar = new HolidayCalendar();

                    // QRTZNET-86
                    var t = scheduler.GetTrigger(new TriggerKey("NonExistingTrigger", "NonExistingGroup"));
                    Assert.IsNull(t);

                    var cal = new AnnualCalendar();
                    scheduler.AddCalendar("annualCalendar", cal, false, true);

                    IOperableTrigger calendarsTrigger = new SimpleTriggerImpl("calendarsTrigger", "test", 20,
                        TimeSpan.FromMilliseconds(5));
                    calendarsTrigger.CalendarName = "annualCalendar";

                    var jd = new JobDetailImpl("testJob", "test", typeof (NoOpJob));
                    scheduler.ScheduleJob(jd, calendarsTrigger);

                    // QRTZNET-93
                    scheduler.AddCalendar("annualCalendar", cal, true, true);

                    scheduler.AddCalendar("baseCalendar", new BaseCalendar(), false, true);
                    scheduler.AddCalendar("cronCalendar", cronCalendar, false, true);
                    scheduler.AddCalendar("dailyCalendar",
                        new DailyCalendar(DateTime.Now.Date, DateTime.Now.AddMinutes(1)), false, true);
                    scheduler.AddCalendar("holidayCalendar", holidayCalendar, false, true);
                    scheduler.AddCalendar("monthlyCalendar", new MonthlyCalendar(), false, true);
                    scheduler.AddCalendar("weeklyCalendar", new WeeklyCalendar(), false, true);

                    scheduler.AddCalendar("cronCalendar", cronCalendar, true, true);
                    scheduler.AddCalendar("holidayCalendar", holidayCalendar, true, true);

                    Assert.IsNotNull(scheduler.GetCalendar("annualCalendar"));

                    var lonelyJob = new JobDetailImpl("lonelyJob", "lonelyGroup", typeof (SimpleRecoveryJob));
                    lonelyJob.Durable = true;
                    lonelyJob.RequestsRecovery = true;
                    scheduler.AddJob(lonelyJob, false);
                    scheduler.AddJob(lonelyJob, true);

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
                    scheduler.ScheduleJob(job, trigger);

                    // check that trigger was stored
                    var persisted = scheduler.GetTrigger(new TriggerKey("trig_" + count, schedId));
                    Assert.IsNotNull(persisted);
                    Assert.IsTrue(persisted is SimpleTriggerImpl);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(5));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(2000L);
                    scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryStatefulJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(3));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(4));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromMilliseconds(4500));
                    scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    IOperableTrigger ct = new CronTriggerImpl("cron_trig_" + count, schedId, "0/10 * * * * ?");
                    ct.JobDataMap.Add("key", "value");
                    ct.StartTimeUtc = DateTime.Now.AddMilliseconds(1000);

                    scheduler.ScheduleJob(job, ct);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    var nt = new DailyTimeIntervalTriggerImpl("nth_trig_" + count, schedId, new TimeOfDay(1, 1, 1),
                        new TimeOfDay(23, 30, 0), IntervalUnit.Hour, 1);
                    nt.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);

                    scheduler.ScheduleJob(job, nt);

                    var nt2 = new DailyTimeIntervalTriggerImpl();
                    nt2.Key = new TriggerKey("nth_trig2_" + count, schedId);
                    nt2.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);
                    nt2.JobKey = job.Key;
                    scheduler.ScheduleJob(nt2);

                    // GitHub issue #92
                    scheduler.GetTrigger(nt2.Key);

                    // GitHub issue #98
                    nt2.StartTimeOfDay = new TimeOfDay(1, 2, 3);
                    nt2.EndTimeOfDay = new TimeOfDay(2, 3, 4);

                    scheduler.UnscheduleJob(nt2.Key);
                    scheduler.ScheduleJob(nt2);

                    var triggerFromDb = (IDailyTimeIntervalTrigger) scheduler.GetTrigger(nt2.Key);
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

                    scheduler.ScheduleJob(intervalTrigger);

                    // bulk operations
                    var info = new System.Collections.Generic.Dictionary<IJobDetail, ISet<ITrigger>>();
                    IJobDetail detail = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    ITrigger simple = new SimpleTriggerImpl("trig_" + count, schedId, 20,
                        TimeSpan.FromMilliseconds(4500));
                    var triggers = new HashSet<ITrigger>();
                    triggers.Add(simple);
                    info[detail] = triggers;

                    scheduler.ScheduleJobs(info, true);

                    Assert.IsTrue(scheduler.CheckExists(detail.Key));
                    Assert.IsTrue(scheduler.CheckExists(simple.Key));

                    // QRTZNET-243
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("a").DeepClone());
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("a").DeepClone());
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("a").DeepClone());
                    scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("a").DeepClone());

                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("a").DeepClone());
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("a").DeepClone());
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("a").DeepClone());
                    scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("a").DeepClone());

                    scheduler.Start();

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    scheduler.PauseAll();

                    scheduler.ResumeAll();

                    scheduler.PauseJob(new JobKey("job_1", schedId));

                    scheduler.ResumeJob(new JobKey("job_1", schedId));

                    scheduler.PauseJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    scheduler.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                    scheduler.PauseTrigger(new TriggerKey("trig_2", schedId));
                    scheduler.ResumeTrigger(new TriggerKey("trig_2", schedId));

                    scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                    Assert.AreEqual(1, scheduler.GetPausedTriggerGroups().Count);

                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                    Assert.IsNotNull(scheduler.GetTrigger(new TriggerKey("trig_2", schedId)));
                    Assert.IsNotNull(scheduler.GetJobDetail(new JobKey("job_1", schedId)));
                    Assert.IsNotNull(scheduler.GetMetaData());
                    Assert.IsNotNull(scheduler.GetCalendar("weeklyCalendar"));

                    var genericjobKey = new JobKey("genericJob", "genericGroup");
                    var genericJob = JobBuilder.Create<GenericJobType<string>>()
                        .WithIdentity(genericjobKey)
                        .StoreDurably()
                        .Build();

                    scheduler.AddJob(genericJob, false);

                    genericJob = scheduler.GetJobDetail(genericjobKey);
                    Assert.That(genericJob, Is.Not.Null);
                    scheduler.TriggerJob(genericjobKey);

                    Thread.Sleep(TimeSpan.FromSeconds(20));

                    Assert.That(GenericJobType<string>.TriggeredCount, Is.EqualTo(1));
                    scheduler.Standby();

                    CollectionAssert.IsNotEmpty(scheduler.GetCalendarNames());
                    CollectionAssert.IsNotEmpty(scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(schedId)));

                    CollectionAssert.IsNotEmpty(scheduler.GetTriggersOfJob(new JobKey("job_2", schedId)));
                    Assert.IsNotNull(scheduler.GetJobDetail(new JobKey("job_2", schedId)));

                    scheduler.DeleteCalendar("cronCalendar");
                    scheduler.DeleteCalendar("holidayCalendar");
                    scheduler.DeleteJob(new JobKey("lonelyJob", "lonelyGroup"));
                    scheduler.DeleteJob(job.Key);

                    scheduler.GetJobGroupNames();
                    scheduler.GetCalendarNames();
                    scheduler.GetTriggerGroupNames();

                    TestMatchers(scheduler);
                }
            }
            finally
            {
                scheduler.Shutdown(false);
            }
        }

        private void TestMatchers(IScheduler scheduler)
        {
            scheduler.Clear();

            var job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "aaabbbccc").StoreDurably().Build();
            scheduler.AddJob(job, true);
            var schedule = SimpleScheduleBuilder.Create();
            var trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "aaabbbccc").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger);

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "xxxyyyzzz").StoreDurably().Build();
            scheduler.AddJob(job, true);
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger);

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job2", "xxxyyyzzz").StoreDurably().Build();
            scheduler.AddJob(job, true);
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig2", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            scheduler.ScheduleJob(trigger);

            var jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());
            Assert.That(jkeys.Count, Is.EqualTo(3), "Wrong number of jobs found by anything matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("xxxyyyzzz"));
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by equals matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("aaabbbccc"));
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by equals matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("aa"));
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by starts with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("xx"));
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by starts with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("cc"));
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by ends with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("zzz"));
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by ends with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("bc"));
            Assert.That(jkeys.Count, Is.EqualTo(1), "Wrong number of jobs found by contains with matcher");

            jkeys = scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("yz"));
            Assert.That(jkeys.Count, Is.EqualTo(2), "Wrong number of jobs found by contains with matcher");

            var tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.AnyGroup());
            Assert.That(tkeys.Count, Is.EqualTo(3), "Wrong number of triggers found by anything matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("xxxyyyzzz"));
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by equals matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("aaabbbccc"));
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by equals matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("aa"));
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by starts with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("xx"));
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by starts with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("cc"));
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by ends with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("zzz"));
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by ends with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("bc"));
            Assert.That(tkeys.Count, Is.EqualTo(1), "Wrong number of triggers found by contains with matcher");

            tkeys = scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("yz"));
            Assert.That(tkeys.Count, Is.EqualTo(2), "Wrong number of triggers found by contains with matcher");
        }
    }

    public class GenericJobType<T> : IJob
    {
        public static int TriggeredCount { get; private set; }

        public void Execute(IJobExecutionContext context)
        {
            TriggeredCount++;
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
        public virtual void Execute(IJobExecutionContext context)
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
        }
    }

    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class SimpleRecoveryStatefulJob : SimpleRecoveryJob
    {
    }
}