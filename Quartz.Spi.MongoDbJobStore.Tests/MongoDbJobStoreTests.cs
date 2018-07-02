using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Tests.Jobs;
using Quartz.Tests.Integration.Impl;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    [TestFixture]
    public class MongoDbJobStoreTests : BaseStoreTests
    {
        private IScheduler _scheduler;

        [SetUp]
        public async Task Setup()
        {
            _scheduler = CreateScheduler();
            await _scheduler.Clear();
        }

        [TearDown]
        public async Task Teardown()
        {
            await _scheduler.Shutdown();
        }

        [Test]
        public void TestStoreInitialization()
        {
            Assert.DoesNotThrow(() => { CreateScheduler(); });
        }

        [Test]
        public async Task AddJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.That(await _scheduler.CheckExists(new JobKey("j1")), Is.False);

            await _scheduler.AddJob(job, false);

            Assert.That(await _scheduler.CheckExists(new JobKey("j1")), Is.True);
        }

        [Test]
        public async Task RetrieveJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();
            await _scheduler.AddJob(job, false);

            job = await _scheduler.GetJobDetail(new JobKey("j1"));

            Assert.That(job, Is.Not.Null);
        }

        [Test]
        public async Task AddTriggerTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            Assert.That(await _scheduler.CheckExists(new TriggerKey("t1")), Is.False);

            await _scheduler.ScheduleJob(job, trigger);

            Assert.That(await _scheduler.CheckExists(new TriggerKey("t1")), Is.True);

            job = await _scheduler.GetJobDetail(new JobKey("j1"));

            Assert.That(job, Is.Not.Null);

            trigger = await _scheduler.GetTrigger(new TriggerKey("t1"));

            Assert.That(trigger, Is.Not.Null);
        }

        [Test]
        public async Task GroupsTest()
        {
            await CreateJobsAndTriggers();

            var jobGroups = await _scheduler.GetJobGroupNames();
            var triggerGroups = await _scheduler.GetTriggerGroupNames();

            Assert.That(jobGroups.Count, Is.EqualTo(2), "Job group list size expected to be = 2 ");
            Assert.That(triggerGroups.Count, Is.EqualTo(2), "Trigger group list size expected to be = 2 ");

            var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            var triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in default group was 1 ");
            Assert.That(triggerKeys.Count, Is.EqualTo(1), "Number of triggers expected in default group was 1 ");

            jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.That(jobKeys.Count, Is.EqualTo(2), "Number of jobs expected in 'g1' group was 2 ");
            Assert.That(triggerKeys.Count, Is.EqualTo(2), "Number of triggers expected in 'g1' group was 2 ");
        }

        [Test]
        public async Task TriggerStateTest()
        {
            await CreateJobsAndTriggers();

            var s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");

            await _scheduler.PauseTrigger(new TriggerKey("t2", "g1"));
            s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t2 expected to be PAUSED ");

            await _scheduler.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");

            var pausedGroups = await _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups, Is.Empty, "Size of paused trigger groups list expected to be 0 ");

            await _scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            // test that adding a trigger to a paused group causes the new trigger to be paused also... 
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j4", "g1")
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t4", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await _scheduler.ScheduleJob(job, trigger);

            pausedGroups = await _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups.Count, Is.EqualTo(1), "Size of paused trigger groups list expected to be 1 ");

            s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t2 expected to be PAUSED ");

            s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t4 expected to be PAUSED ");

            await _scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));
            s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");
            s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t4 expected to be NORMAL ");
            pausedGroups = await _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups, Is.Empty, "Size of paused trigger groups list expected to be 0 ");
        }

        [Test]
        public async Task SchedulingTest()
        {
            await CreateJobsAndTriggers();

            Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk")), Is.False,
                "Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

            Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("t3", "g1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            var triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in 'g1' group was 1 ");
            // job should have been deleted also, because it is non-durable
            Assert.That(triggerKeys.Count, Is.EqualTo(1), "Number of triggers expected in 'g1' group was 1 ");

            Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("t1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in default group was 1 ");
            // job should have been left in place, because it is non-durable
            Assert.That(triggerKeys, Is.Empty, "Number of triggers expected in default group was 0 ");
        }

        [Test]
        public async Task SimpleReschedulingTest()
        {
            var job = JobBuilder.Create<SimpleJob>().WithIdentity("job1", "group1").Build();
            var trigger1 = TriggerBuilder.Create()
                .ForJob(job)
                .WithIdentity("trigger1", "group1")
                .StartAt(DateTimeOffset.Now.AddSeconds(30))
                .Build();

            await _scheduler.ScheduleJob(job, trigger1);

            job = await _scheduler.GetJobDetail(job.Key);
            Assert.That(job, Is.Not.Null);

            var trigger2 = TriggerBuilder.Create()
                .ForJob(job)
                .WithIdentity("trigger1", "group1")
                .StartAt(DateTimeOffset.Now.AddSeconds(60))
                .Build();
            await _scheduler.RescheduleJob(trigger1.Key, trigger2);
            job = await _scheduler.GetJobDetail(job.Key);
            Assert.That(job, Is.Not.Null);
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedBefore()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);
            await _scheduler.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<SimpleJobWithSync>()
                .WithIdentity("job1")
                .Build();

            var trigger1 = TriggerBuilder.Create()
                .ForJob(job1)
                .Build();

            var sTime = DateTime.UtcNow;

            await _scheduler.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(TestTimeout);

            await _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            await _scheduler.Clear();

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);

            await _scheduler.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<SimpleJobWithSync>()
                .WithIdentity("job1").
                StoreDurably().Build();
            await _scheduler.AddJob(job1, false);

            var sTime = DateTime.UtcNow;

            await _scheduler.TriggerJob(job1.Key);

            barrier.SignalAndWait(TestTimeout);

            await _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
            // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedAfter()
        {
            var jobExecTimestamps = new List<DateTime>();

            var barrier = new Barrier(2);

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);

            var job1 = JobBuilder.Create<SimpleJobWithSync>().WithIdentity("job1").Build();
            var trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

            var sTime = DateTime.UtcNow;

            await _scheduler.ScheduleJob(job1, trigger1);
            await _scheduler.Start();

            barrier.SignalAndWait(TestTimeout);

            await _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
            // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public async Task TestScheduleMultipleTriggersForAJob()
        {
            var job = JobBuilder.Create<SimpleJob>().WithIdentity("job1", "group1").Build();
            var trigger1 = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();
            var trigger2 = TriggerBuilder.Create()
                .WithIdentity("trigger2", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();

            var triggersForJob = (IReadOnlyCollection<ITrigger>)new HashSet<ITrigger>{trigger1, trigger2};

            await _scheduler.ScheduleJob(job, triggersForJob, true);

            var triggersOfJob = await _scheduler.GetTriggersOfJob(job.Key);
            Assert.That(triggersOfJob.Count, Is.EqualTo(2));
            Assert.That(triggersOfJob.Contains(trigger1));
            Assert.That(triggersOfJob.Contains(trigger2));

            await _scheduler.Shutdown(false);
        }

        [Test]
        public async Task TestDurableStorageFunctions()
        {
            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.That(await _scheduler.CheckExists(new JobKey("j1")), Is.False, "Unexpected existence of job named 'j1'.");

            await _scheduler.AddJob(job, false);

            Assert.That(await _scheduler.CheckExists(new JobKey("j1")), "Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                await _scheduler.AddJob(nonDurableJob, false);
                Assert.Fail("Storage of non-durable job should not have succeeded.");
            }
            catch (AggregateException e)
            {
                var expectedException = e.InnerException as SchedulerException;
                Assert.That(expectedException, Is.Not.Null);
                Assert.That(await _scheduler.CheckExists(new JobKey("j2")), Is.False,
                    "Unexpected existence of job named 'j2'.");
            }

            await _scheduler.AddJob(nonDurableJob, false, true);

            Assert.That(await _scheduler.CheckExists(new JobKey("j2")), "Unexpected non-existence of job named 'j2'.");
        }

        [Test]
        public async Task TestShutdownWithoutWaitIsUnclean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            try
            {
                _scheduler.Context.Put(Barrier, barrier);
                _scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await _scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await _scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                await _scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await _scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                await _scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(TestTimeout);
        }

        [Test]
        public async Task TestShutdownWithWaitIsClean()
        {
            var shutdown = false;
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            try
            {
                _scheduler.Context.Put(Barrier, barrier);
                _scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await _scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await _scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                await _scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await _scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                void ThreadStart()
                {
                    try
                    {
                        _scheduler.Shutdown(true);
                        shutdown = true;
                    }
                    catch (SchedulerException ex)
                    {
                        throw new Exception("exception: " + ex.Message, ex);
                    }
                }

                var t = new Thread(ThreadStart);
                t.Start();
                Thread.Sleep(1000);
                Assert.That(shutdown, Is.False);
                barrier.SignalAndWait(TestTimeout);
                t.Join();
            }
        }

        [Test]
        public void SmokeTest()
        {
            new SmokeTestPerformer().Test(_scheduler, true, true);
        }

        private async Task CreateJobsAndTriggers()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await _scheduler.ScheduleJob(job, trigger);

            job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t2", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await _scheduler.ScheduleJob(job, trigger);

            job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j3", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t3", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await _scheduler.ScheduleJob(job, trigger);
        }
    }
}