using System;
using System.Collections.Generic;
using System.Threading;
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
        public void Setup()
        {
            _scheduler = CreateScheduler();
            _scheduler.Clear();
        }

        [TearDown]
        public void Teardown()
        {
            _scheduler.Shutdown();
        }

        [Test]
        public void TestStoreInitialization()
        {
            Assert.DoesNotThrow(() => { CreateScheduler(); });
        }

        [Test]
        public void AddJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.That(_scheduler.CheckExists(new JobKey("j1")), Is.False);

            _scheduler.AddJob(job, false);

            Assert.That(_scheduler.CheckExists(new JobKey("j1")), Is.True);
        }

        [Test]
        public void RetrieveJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();
            _scheduler.AddJob(job, false);

            job = _scheduler.GetJobDetail(new JobKey("j1"));

            Assert.That(job, Is.Not.Null);
        }

        [Test]
        public void AddTriggerTest()
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

            Assert.That(_scheduler.CheckExists(new TriggerKey("t1")), Is.False);

            _scheduler.ScheduleJob(job, trigger);

            Assert.That(_scheduler.CheckExists(new TriggerKey("t1")), Is.True);

            job = _scheduler.GetJobDetail(new JobKey("j1"));

            Assert.That(job, Is.Not.Null);

            trigger = _scheduler.GetTrigger(new TriggerKey("t1"));

            Assert.That(trigger, Is.Not.Null);
        }

        [Test]
        public void GroupsTest()
        {
            CreateJobsAndTriggers();

            var jobGroups = _scheduler.GetJobGroupNames();
            var triggerGroups = _scheduler.GetTriggerGroupNames();

            Assert.That(jobGroups.Count, Is.EqualTo(2), "Job group list size expected to be = 2 ");
            Assert.That(triggerGroups.Count, Is.EqualTo(2), "Trigger group list size expected to be = 2 ");

            var jobKeys = _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            var triggerKeys = _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in default group was 1 ");
            Assert.That(triggerKeys.Count, Is.EqualTo(1), "Number of triggers expected in default group was 1 ");

            jobKeys = _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.That(jobKeys.Count, Is.EqualTo(2), "Number of jobs expected in 'g1' group was 2 ");
            Assert.That(triggerKeys.Count, Is.EqualTo(2), "Number of triggers expected in 'g1' group was 2 ");
        }

        [Test]
        public void TriggerStateTest()
        {
            CreateJobsAndTriggers();

            var s = _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");

            _scheduler.PauseTrigger(new TriggerKey("t2", "g1"));
            s = _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t2 expected to be PAUSED ");

            _scheduler.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");

            var pausedGroups = _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups, Is.Empty, "Size of paused trigger groups list expected to be 0 ");

            _scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

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

            _scheduler.ScheduleJob(job, trigger);

            pausedGroups = _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups.Count, Is.EqualTo(1), "Size of paused trigger groups list expected to be 1 ");

            s = _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t2 expected to be PAUSED ");

            s = _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.That(s.Equals(TriggerState.Paused), "State of trigger t4 expected to be PAUSED ");

            _scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));
            s = _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t2 expected to be NORMAL ");
            s = _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.That(s.Equals(TriggerState.Normal), "State of trigger t4 expected to be NORMAL ");
            pausedGroups = _scheduler.GetPausedTriggerGroups();
            Assert.That(pausedGroups, Is.Empty, "Size of paused trigger groups list expected to be 0 ");
        }

        [Test]
        public void SchedulingTest()
        {
            CreateJobsAndTriggers();

            Assert.That(_scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk")), Is.False,
                "Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

            Assert.That(_scheduler.UnscheduleJob(new TriggerKey("t3", "g1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            var jobKeys = _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            var triggerKeys = _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in 'g1' group was 1 ");
            // job should have been deleted also, because it is non-durable
            Assert.That(triggerKeys.Count, Is.EqualTo(1), "Number of triggers expected in 'g1' group was 1 ");

            Assert.That(_scheduler.UnscheduleJob(new TriggerKey("t1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            triggerKeys = _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.That(jobKeys.Count, Is.EqualTo(1), "Number of jobs expected in default group was 1 ");
            // job should have been left in place, because it is non-durable
            Assert.That(triggerKeys, Is.Empty, "Number of triggers expected in default group was 0 ");
        }

        [Test]
        public void TestAbilityToFireImmediatelyWhenStartedBefore()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);
            _scheduler.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<SimpleJobWithSync>()
                .WithIdentity("job1")
                .Build();

            var trigger1 = TriggerBuilder.Create()
                .ForJob(job1)
                .Build();

            var sTime = DateTime.UtcNow;

            _scheduler.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(TestTimeout);

            _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Test]
        public void TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            _scheduler.Clear();

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);

            _scheduler.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<SimpleJobWithSync>()
                .WithIdentity("job1").
                StoreDurably().Build();
            _scheduler.AddJob(job1, false);

            var sTime = DateTime.UtcNow;

            _scheduler.TriggerJob(job1.Key);

            barrier.SignalAndWait(TestTimeout);

            _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
            // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public void TestAbilityToFireImmediatelyWhenStartedAfter()
        {
            var jobExecTimestamps = new List<DateTime>();

            var barrier = new Barrier(2);

            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);

            var job1 = JobBuilder.Create<SimpleJobWithSync>().WithIdentity("job1").Build();
            var trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

            var sTime = DateTime.UtcNow;

            _scheduler.ScheduleJob(job1, trigger1);
            _scheduler.Start();

            barrier.SignalAndWait(TestTimeout);

            _scheduler.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
            // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public void TestScheduleMultipleTriggersForAJob()
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

            Collection.ISet<ITrigger> triggersForJob = new Collection.HashSet<ITrigger>();
            triggersForJob.Add(trigger1);
            triggersForJob.Add(trigger2);

            _scheduler.ScheduleJob(job, triggersForJob, true);

            var triggersOfJob = _scheduler.GetTriggersOfJob(job.Key);
            Assert.That(triggersOfJob.Count, Is.EqualTo(2));
            Assert.That(triggersOfJob.Contains(trigger1));
            Assert.That(triggersOfJob.Contains(trigger2));

            _scheduler.Shutdown(false);
        }

        [Test]
        public void TestDurableStorageFunctions()
        {
            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.That(_scheduler.CheckExists(new JobKey("j1")), Is.False, "Unexpected existence of job named 'j1'.");

            _scheduler.AddJob(job, false);

            Assert.That(_scheduler.CheckExists(new JobKey("j1")), "Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                _scheduler.AddJob(nonDurableJob, false);
                Assert.Fail("Storage of non-durable job should not have succeeded.");
            }
            catch (SchedulerException)
            {
                Assert.That(_scheduler.CheckExists(new JobKey("j2")), Is.False,
                    "Unexpected existence of job named 'j2'.");
            }

            _scheduler.AddJob(nonDurableJob, false, true);

            Assert.That(_scheduler.CheckExists(new JobKey("j2")), "Unexpected non-existence of job named 'j2'.");
        }

        [Test]
        public void TestShutdownWithoutWaitIsUnclean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            try
            {
                _scheduler.Context.Put(Barrier, barrier);
                _scheduler.Context.Put(DateStamps, jobExecTimestamps);
                _scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                _scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                _scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while (_scheduler.GetCurrentlyExecutingJobs().Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                _scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(TestTimeout);
        }

        [Test]
        public void TestShutdownWithWaitIsClean()
        {
            var shutdown = false;
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            try
            {
                _scheduler.Context.Put(Barrier, barrier);
                _scheduler.Context.Put(DateStamps, jobExecTimestamps);
                _scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                _scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                _scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while (_scheduler.GetCurrentlyExecutingJobs().Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                ThreadStart threadStart = () =>
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
                };

                var t = new Thread(threadStart);
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

        private void CreateJobsAndTriggers()
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

            _scheduler.ScheduleJob(job, trigger);

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

            _scheduler.ScheduleJob(job, trigger);

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

            _scheduler.ScheduleJob(job, trigger);
        }
    }
}