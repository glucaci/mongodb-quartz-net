using System;
using System.Threading;
using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
//using Quartz.Job;
using Quartz.Spi;
using Quartz.Util;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace Quartz.Tests.Integration.Impl
{
    public class SmokeTestPerformer
    {
        public async Task Test(IScheduler scheduler, bool clearJobs, bool scheduleJobs)
        {
            try
            {
                if (clearJobs)
                {
                    await scheduler.Clear();
                }

                if (scheduleJobs)
                {
                    ICalendar cronCalendar = new CronCalendar("0/5 * * * * ?");
                    ICalendar holidayCalendar = new HolidayCalendar();

                    // QRTZNET-86
                    var t = await scheduler.GetTrigger(new TriggerKey("NonExistingTrigger", "NonExistingGroup"));
                    (t).Should().BeNull();

                    var cal = new AnnualCalendar();
                    await scheduler.AddCalendar("annualCalendar", cal, false, true);

                    IOperableTrigger calendarsTrigger = new SimpleTriggerImpl("calendarsTrigger", "test", 20,
                        TimeSpan.FromMilliseconds(5));
                    calendarsTrigger.CalendarName = "annualCalendar";

                    var jd = new JobDetailImpl("testJob", "test", typeof (NoOpJob));
                    await scheduler.ScheduleJob(jd, calendarsTrigger);

                    // QRTZNET-93
                    await scheduler.AddCalendar("annualCalendar", cal, true, true);

                    await scheduler.AddCalendar("baseCalendar", new BaseCalendar(), false, true);
                    await scheduler.AddCalendar("cronCalendar", cronCalendar, false, true);
                    await scheduler.AddCalendar("dailyCalendar",
                        new DailyCalendar(DateTime.Now.Date, DateTime.Now.AddMinutes(1)), false, true);
                    await scheduler.AddCalendar("holidayCalendar", holidayCalendar, false, true);
                    await scheduler.AddCalendar("monthlyCalendar", new MonthlyCalendar(), false, true);
                    await scheduler.AddCalendar("weeklyCalendar", new WeeklyCalendar(), false, true);

                    await scheduler.AddCalendar("cronCalendar", cronCalendar, true, true);
                    await scheduler.AddCalendar("holidayCalendar", holidayCalendar, true, true);

                    (await scheduler.GetCalendar("annualCalendar")).Should().NotBeNull();

                    var lonelyJob = new JobDetailImpl("lonelyJob", "lonelyGroup", typeof (SimpleRecoveryJob));
                    lonelyJob.Durable = true;
                    lonelyJob.RequestsRecovery = true;
                    await scheduler.AddJob(lonelyJob, false);
                    await scheduler.AddJob(lonelyJob, true);

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
                    await scheduler.ScheduleJob(job, trigger);

                    // check that trigger was stored
                    var persisted = await scheduler.GetTrigger(new TriggerKey("trig_" + count, schedId));
                    (persisted).Should().NotBeNull();
                    (persisted is SimpleTriggerImpl).Should().BeTrue();

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(5));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(2000L);
                    await scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryStatefulJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(3));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    await scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(4));

                    trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                    await scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromMilliseconds(4500));
                    await scheduler.ScheduleJob(job, trigger);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    IOperableTrigger ct = new CronTriggerImpl("cron_trig_" + count, schedId, "0/10 * * * * ?");
                    ct.JobDataMap.Add("key", "value");
                    ct.StartTimeUtc = DateTime.Now.AddMilliseconds(1000);

                    await scheduler.ScheduleJob(job, ct);

                    count++;
                    job = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    job.RequestsRecovery = true;
                    var nt = new DailyTimeIntervalTriggerImpl("nth_trig_" + count, schedId, new TimeOfDay(1, 1, 1),
                        new TimeOfDay(23, 30, 0), IntervalUnit.Hour, 1);
                    nt.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);

                    await scheduler.ScheduleJob(job, nt);

                    var nt2 = new DailyTimeIntervalTriggerImpl();
                    nt2.Key = new TriggerKey("nth_trig2_" + count, schedId);
                    nt2.StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000);
                    nt2.JobKey = job.Key;
                    await scheduler.ScheduleJob(nt2);

                    // GitHub issue #92
                    await scheduler.GetTrigger(nt2.Key);

                    // GitHub issue #98
                    nt2.StartTimeOfDay = new TimeOfDay(1, 2, 3);
                    nt2.EndTimeOfDay = new TimeOfDay(2, 3, 4);

                    await scheduler.UnscheduleJob(nt2.Key);
                    await scheduler.ScheduleJob(nt2);

                    var triggerFromDb = (IDailyTimeIntervalTrigger) await scheduler.GetTrigger(nt2.Key);
                    triggerFromDb.StartTimeOfDay.Hour.Should().Be(1);
                    triggerFromDb.StartTimeOfDay.Minute.Should().Be(2);
                    triggerFromDb.StartTimeOfDay.Second.Should().Be(3);

                    triggerFromDb.EndTimeOfDay.Hour.Should().Be(2);
                    triggerFromDb.EndTimeOfDay.Minute.Should().Be(3);
                    triggerFromDb.EndTimeOfDay.Second.Should().Be(4);

                    job.RequestsRecovery = true;
                    var intervalTrigger = new CalendarIntervalTriggerImpl(
                        "calint_trig_" + count,
                        schedId,
                        DateTime.UtcNow.AddMilliseconds(300),
                        DateTime.UtcNow.AddMinutes(1),
                        IntervalUnit.Second,
                        8);
                    intervalTrigger.JobKey = job.Key;

                    await scheduler.ScheduleJob(intervalTrigger);

                    // bulk operations
                    IJobDetail detail = new JobDetailImpl("job_" + count, schedId, typeof (SimpleRecoveryJob));
                    ITrigger simple = new SimpleTriggerImpl("trig_" + count, schedId, 20,
                        TimeSpan.FromMilliseconds(4500));
                    var triggers = (IReadOnlyCollection<ITrigger>)new HashSet<ITrigger> {simple};
                    var info = (IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>>)new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
                    {
                        [detail] = triggers
                    };

                    await scheduler.ScheduleJobs(info, true);

                    (await scheduler.CheckExists(detail.Key)).Should().BeTrue();
                    (await scheduler.CheckExists(simple.Key)).Should().BeTrue();

                    // QRTZNET-243
                    await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("a").DeepClone());
                    await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("a").DeepClone());
                    await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("a").DeepClone());
                    await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("a").DeepClone());

                    await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("a").DeepClone());
                    await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("a").DeepClone());
                    await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("a").DeepClone());
                    await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("a").DeepClone());

                    await scheduler.Start();

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    await scheduler.PauseAll();

                    await scheduler.ResumeAll();

                    await scheduler.PauseJob(new JobKey("job_1", schedId));

                    await scheduler.ResumeJob(new JobKey("job_1", schedId));

                    await scheduler.PauseJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    await scheduler.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                    await scheduler.PauseTrigger(new TriggerKey("trig_2", schedId));
                    await scheduler.ResumeTrigger(new TriggerKey("trig_2", schedId));

                    await scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                    (await scheduler.GetPausedTriggerGroups()).Count.Should().Be(1);

                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    await scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                    (await scheduler.GetTrigger(new TriggerKey("trig_2", schedId))).Should().NotBeNull();
                    (await scheduler.GetJobDetail(new JobKey("job_1", schedId))).Should().NotBeNull();
                    (await scheduler.GetMetaData()).Should().NotBeNull();
                    (await scheduler.GetCalendar("weeklyCalendar")).Should().NotBeNull();

                    var genericjobKey = new JobKey("genericJob", "genericGroup");
                    var genericJob = JobBuilder.Create<GenericJobType<string>>()
                        .WithIdentity(genericjobKey)
                        .StoreDurably()
                        .Build();

                    await scheduler.AddJob(genericJob, false);

                    genericJob = await scheduler.GetJobDetail(genericjobKey);
                    genericJob.Should().NotBeNull();
                    await scheduler.TriggerJob(genericjobKey);

                    Thread.Sleep(TimeSpan.FromSeconds(20));

                    GenericJobType<string>.TriggeredCount.Should().Be(1);
                    await scheduler.Standby();

                    (await scheduler.GetCalendarNames()).Should().NotBeEmpty();
                    (await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(schedId))).Should().NotBeEmpty();
                    (await scheduler.GetTriggersOfJob(new JobKey("job_2", schedId))).Should().NotBeEmpty();
                    (await scheduler.GetJobDetail(new JobKey("job_2", schedId))).Should().NotBeNull();

                    await scheduler.DeleteCalendar("cronCalendar");
                    await scheduler.DeleteCalendar("holidayCalendar");
                    await scheduler.DeleteJob(new JobKey("lonelyJob", "lonelyGroup"));
                    await scheduler.DeleteJob(job.Key);

                    await scheduler.GetJobGroupNames();
                    await scheduler.GetCalendarNames();
                    await scheduler.GetTriggerGroupNames();

                    await TestMatchers(scheduler);
                }
            }
            finally
            {
                await scheduler.Shutdown(false);
            }
        }

        private async Task TestMatchers(IScheduler scheduler)
        {
            await scheduler.Clear();

            var job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "aaabbbccc").StoreDurably().Build();
            await scheduler.AddJob(job, true);
            var schedule = SimpleScheduleBuilder.Create();
            var trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "aaabbbccc").WithSchedule(schedule).ForJob(job).Build();
            await scheduler.ScheduleJob(trigger);

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job1", "xxxyyyzzz").StoreDurably().Build();
            await scheduler.AddJob(job, true);
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig1", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            await scheduler.ScheduleJob(trigger);

            job = JobBuilder.Create<NoOpJob>().WithIdentity("job2", "xxxyyyzzz").StoreDurably().Build();
            await scheduler.AddJob(job, true);
            schedule = SimpleScheduleBuilder.Create();
            trigger =
                TriggerBuilder.Create().WithIdentity("trig2", "xxxyyyzzz").WithSchedule(schedule).ForJob(job).Build();
            await scheduler.ScheduleJob(trigger);

            var jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());
            jkeys.Count.Should().Be(3, "Wrong number of jobs found by anything matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("xxxyyyzzz"));
            jkeys.Count.Should().Be(2, "Wrong number of jobs found by equals matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("aaabbbccc"));
            jkeys.Count.Should().Be(1, "Wrong number of jobs found by equals matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("aa"));
            jkeys.Count.Should().Be(1, "Wrong number of jobs found by starts with matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("xx"));
            jkeys.Count.Should().Be(2, "Wrong number of jobs found by starts with matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("cc"));
            jkeys.Count.Should().Be(1, "Wrong number of jobs found by ends with matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("zzz"));
            jkeys.Count.Should().Be(2, "Wrong number of jobs found by ends with matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("bc"));
            jkeys.Count.Should().Be(1, "Wrong number of jobs found by contains with matcher");

            jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("yz"));
            jkeys.Count.Should().Be(2, "Wrong number of jobs found by contains with matcher");

            var tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.AnyGroup());
            tkeys.Count.Should().Be(3, "Wrong number of triggers found by anything matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("xxxyyyzzz"));
            tkeys.Count.Should().Be(2, "Wrong number of triggers found by equals matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("aaabbbccc"));
            tkeys.Count.Should().Be(1, "Wrong number of triggers found by equals matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("aa"));
            tkeys.Count.Should().Be(1, "Wrong number of triggers found by starts with matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("xx"));
            tkeys.Count.Should().Be(2, "Wrong number of triggers found by starts with matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("cc"));
            tkeys.Count.Should().Be(1, "Wrong number of triggers found by ends with matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("zzz"));
            tkeys.Count.Should().Be(2, "Wrong number of triggers found by ends with matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("bc"));
            tkeys.Count.Should().Be(1, "Wrong number of triggers found by contains with matcher");

            tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("yz"));
            tkeys.Count.Should().Be(2, "Wrong number of triggers found by contains with matcher");
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