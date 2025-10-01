using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22829_DeferrableTimerTests : NoDisposalNeeded
    {
        public RavenDB_22829_DeferrableTimerTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(1)]
        [InlineData(10)]
        public async Task ScheduleOnceDeferManyConcurrently(int promises)
        {
            var tasks = new List<Task>();
            for (int i = 0; i < promises; i++)
            {
                var t = ScheduleOnceDeferMany();
                tasks.Add(t);
            }
            await Task.WhenAll(tasks);
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanReusePromise()
        {
            var counter = new State();
            var p = new DeferrableTimeout.Promise(TimeSpan.FromMilliseconds(100));
            for (int i = 1; i < 10; i++)
            {
                await ScheduleOnceDeferMany(counter, p);
                Assert.Equal(i, counter.Count);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task ReScheduleWhileWorking()
        {
            using (var tcs = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
            {
                var p = new DeferrableTimeout.Promise(TimeSpan.FromMilliseconds(100));
                
                var counter = new State();
                _ = ScheduleOnceDeferMany(counter, p);
                await counter.WaitUntilScheduled.WaitAsync(tcs.Token);

                var counter2 = new State();
                var t = ScheduleOrDefer(p, counter2);
                await counter2.WaitUntilScheduled.WaitAsync(tcs.Token);
                var r = await t;
                Assert.Equal(DeferrableTimeout.Promise.Result.Scheduled, r);
            }
        }

        private static Task ScheduleOnceDeferMany() => ScheduleOnceDeferMany(new State(), new DeferrableTimeout.Promise(TimeSpan.FromMilliseconds(100)));

        private static async Task ScheduleOnceDeferMany(State state, DeferrableTimeout.Promise p)
        {
            var tasks = new List<Task<DeferrableTimeout.Promise.Result>>();
            for (int j = 0; j < 10; j++)
            {
                var t = Task.Run(() => ScheduleOrDefer(p, state));
                tasks.Add(t);
            }

            await Task.WhenAll(tasks);

            var dic = new Dictionary<DeferrableTimeout.Promise.Result, int>
            {
                [DeferrableTimeout.Promise.Result.Deferred] = 0,
                [DeferrableTimeout.Promise.Result.Scheduled] = 0,
            };
            foreach (var task in tasks)
            {
                var r = await task;
                dic[r]++;
            }
            Assert.Equal(1, dic[DeferrableTimeout.Promise.Result.Scheduled]);
            Assert.Equal(9, dic[DeferrableTimeout.Promise.Result.Deferred]);
        }

        private static async Task<DeferrableTimeout.Promise.Result> ScheduleOrDefer(DeferrableTimeout.Promise p, State state)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10));
            var r = p.ScheduleOrDefer(out var t);
            switch (r)
            {
                case DeferrableTimeout.Promise.Result.Scheduled:
                    await t;
                    break;
                case DeferrableTimeout.Promise.Result.Deferred:
                    return DeferrableTimeout.Promise.Result.Deferred;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            state.WaitUntilScheduled.Set();
            await Task.Delay(TimeSpan.FromMilliseconds(100)); // simulate some work
            state.Count++; // this should be thread safe to do
            state.WaitUntilScheduled.Reset();

            p.Reset();
            return DeferrableTimeout.Promise.Result.Scheduled;
        }

        private class State
        {
            public int Count;
            public AsyncManualResetEvent WaitUntilScheduled = new AsyncManualResetEvent(false);
        }
    }
}
