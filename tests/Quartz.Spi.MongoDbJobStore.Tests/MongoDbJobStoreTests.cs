using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Quartz.Impl.Matchers;
using Quartz.Spi.MongoDbJobStore.Tests.Jobs;
using Quartz.Tests.Integration.Impl;
using FluentAssertions;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    public class MongoDbJobStoreTests : BaseStoreTests, IDisposable
    {
        private IScheduler _scheduler;

        public MongoDbJobStoreTests()
        {
            _scheduler = CreateScheduler().Result;
            _scheduler.Clear().Wait();
        }

        public void Dispose()
        {
            _scheduler.Shutdown().Wait();
        }

        [Fact]
        public async Task AddJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            (await _scheduler.CheckExists(new JobKey("j1"))).Should().BeFalse();

            await _scheduler.AddJob(job, false);

            (await _scheduler.CheckExists(new JobKey("j1"))).Should().BeTrue();
        }

        [Fact]
        public async Task RetrieveJobTest()
        {
           var job = JobBuilder.Create<SimpleJob>()
               .WithIdentity("j1")
               .StoreDurably()
               .Build();
           await _scheduler.AddJob(job, false);

           job = await _scheduler.GetJobDetail(new JobKey("j1"));

           job.Should().NotBeNull();
        }

        [Fact]
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

           (await _scheduler.CheckExists(new TriggerKey("t1"))).Should().BeFalse();

           await _scheduler.ScheduleJob(job, trigger);

           (await _scheduler.CheckExists(new TriggerKey("t1"))).Should().BeTrue();

           job = await _scheduler.GetJobDetail(new JobKey("j1"));

           job.Should().NotBeNull();

           trigger = await _scheduler.GetTrigger(new TriggerKey("t1"));

           trigger.Should().NotBeNull();
        }

        [Fact]
        public async Task GroupsTest()
        {
           await CreateJobsAndTriggers();

           var jobGroups = await _scheduler.GetJobGroupNames();
           var triggerGroups = await _scheduler.GetTriggerGroupNames();

           jobGroups.Count.Should().Be(2, "Job group list size expected to be = 2 ");
           triggerGroups.Count.Should().Be(2, "Trigger group list size expected to be = 2 ");

           var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
           var triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in default group was 1 ");
           triggerKeys.Count.Should().Be(1, "Number of triggers expected in default group was 1 ");

           jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
           triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

           jobKeys.Count.Should().Be(2, "Number of jobs expected in 'g1' group was 2 ");
           triggerKeys.Count.Should().Be(2, "Number of triggers expected in 'g1' group was 2 ");
        }

        [Fact]
        public async Task TriggerStateTest()
        {
           await CreateJobsAndTriggers();

           var s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");

           await _scheduler.PauseTrigger(new TriggerKey("t2", "g1"));
           s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t2 expected to be PAUSED ");

           await _scheduler.ResumeTrigger(new TriggerKey("t2", "g1"));
           s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");

           var pausedGroups = await _scheduler.GetPausedTriggerGroups();
           (pausedGroups).Should().BeEmpty("Size of paused trigger groups list expected to be 0 ");

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
           pausedGroups.Count.Should().Be(1, "Size of paused trigger groups list expected to be 1 ");

           s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t2 expected to be PAUSED ");

           s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t4 expected to be PAUSED ");

           await _scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));
           s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");
           s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t4 expected to be NORMAL ");
           pausedGroups = await _scheduler.GetPausedTriggerGroups();
           (pausedGroups).Should().BeEmpty("Size of paused trigger groups list expected to be 0 ");
        }

        [Fact]
        public async Task SchedulingTest()
        {
           await CreateJobsAndTriggers();

           (await _scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk"))).Should().BeFalse("Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

           (await _scheduler.UnscheduleJob(new TriggerKey("t3", "g1"))).Should()
               .BeTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

           var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
           var triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in 'g1' group was 1 ");
           // job should have been deleted also, because it is non-durable
           triggerKeys.Count.Should().Be(1, "Number of triggers expected in 'g1' group was 1 ");

           (await _scheduler.UnscheduleJob(new TriggerKey("t1"))).Should().BeTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

           jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
           triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in default group was 1 ");
           // job should have been left in place, because it is non-durable
           (triggerKeys).Should().BeEmpty("Number of triggers expected in default group was 0 ");
        }

        [Fact]
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
           job.Should().NotBeNull();

           var trigger2 = TriggerBuilder.Create()
               .ForJob(job)
               .WithIdentity("trigger1", "group1")
               .StartAt(DateTimeOffset.Now.AddSeconds(60))
               .Build();
           await _scheduler.RescheduleJob(trigger1.Key, trigger2);
           job = await _scheduler.GetJobDetail(job.Key);
           job.Should().NotBeNull();
        }

        [Fact]
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

           (fTime - sTime < TimeSpan.FromMilliseconds(7000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Fact]
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

           (fTime - sTime < TimeSpan.FromMilliseconds(7000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
           // This is dangerously subjective!  but what else to do?
        }

        [Fact]
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

           (fTime - sTime < TimeSpan.FromMilliseconds(7000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
           // This is dangerously subjective!  but what else to do?
        }

        [Fact]
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
           triggersOfJob.Count.Should().Be(2);
           (triggersOfJob.Contains(trigger1)).Should().BeTrue();
           (triggersOfJob.Contains(trigger2)).Should().BeTrue();

           await _scheduler.Shutdown(false);
        }

        [Fact]
        public async Task TestDurableStorageFunctions()
        {
            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            (await _scheduler.CheckExists(new JobKey("j1"))).Should().BeFalse("Unexpected existence of job named 'j1'.");

            await _scheduler.AddJob(job, false);

            (await _scheduler.CheckExists(new JobKey("j1"))).Should().BeTrue("Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                await _scheduler.AddJob(nonDurableJob, false);
                throw new Exception("Storage of non-durable job should not have succeeded.");
            }
            catch (Exception e)
            {
                var expectedException = e as SchedulerException;
                expectedException.Should().NotBeNull();
                (await _scheduler.CheckExists(new JobKey("j2"))).Should().BeFalse("Unexpected existence of job named 'j2'.");
            }

            await _scheduler.AddJob(nonDurableJob, false, true);

            (await _scheduler.CheckExists(new JobKey("j2"))).Should().BeTrue("Unexpected non-existence of job named 'j2'.");
        }

        [Fact]
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

        [Fact]
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
                var task = Task.Run(async () =>
                {
                    try
                    {
                        await _scheduler.Shutdown(true);
                        shutdown = true;
                    }
                    catch (SchedulerException ex)
                    {
                        throw new Exception("exception: " + ex.Message, ex);
                    }
                });
                Thread.Sleep(1000);
                shutdown.Should().BeFalse();
                barrier.SignalAndWait(TestTimeout);
                task.Wait();
            }
        }

        [Fact]
        public async Task SmokeTest()
        {
            await new SmokeTestPerformer().Test(_scheduler, true, true);
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