using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Server.Utils.DiskStatsGetter;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Monitoring
{
    public class DiskStatsGetterTest : NoDisposalNeeded
    {
        public DiskStatsGetterTest(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildMultiplatformFact(RavenPlatform.Linux)]
        public async Task LinuxDiskStats_WhenGetInParallel_ShouldTakeTheSameAsSequential()
        {
#pragma warning disable CA1416
            var diskStatsGetter = new LinuxDiskStatsGetter(TimeSpan.FromMilliseconds(100));
#pragma warning restore CA1416
            await DiskStats_WhenGetInParallel_ShouldTakeTheSameAsSequential(diskStatsGetter);
        }

        [NightlyBuildMultiplatformFact(RavenPlatform.Windows)]
        public async Task WindowsDiskStats_WhenGetInParallel_ShouldTakeTheSameAsSequential()
        {
#pragma warning disable CA1416
            var diskStatsGetter = new WindowsDiskStatsGetter(TimeSpan.FromMilliseconds(100));
#pragma warning restore CA1416
            await DiskStats_WhenGetInParallel_ShouldTakeTheSameAsSequential(diskStatsGetter);
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public async Task DiskStats_WhenTakesLongTime_ShouldReturnPrevResult()
        {
            const string drive = "C:";

            using var mre = new ManualResetEvent(false);
            var statsGetter = new DummyDiskStatsGetter(TimeSpan.FromMicroseconds(1), mre);
            Assert.Null(await statsGetter.GetAsync(drive));
            Assert.Null(await statsGetter.GetAsync(drive));

            mre.Set();
            await WaitFor(async () => (await statsGetter.GetAsync(drive))?.QueueLength >= 1);
            mre.Reset();

            await statsGetter.GetAsync(drive);
            var result = await statsGetter.GetAsync(drive);
            await Task.Delay(100);
            var result2 = await statsGetter.GetAsync(drive);

            Assert.Equal(result.QueueLength, result2.QueueLength);

            mre.Set();
            await WaitFor(async () => (await statsGetter.GetAsync(drive))?.QueueLength >= result.QueueLength);
        }

        private static async Task WaitFor(Func<Task<bool>> func)
        {
            var stop = Stopwatch.StartNew();
            while (true)
            {
                if (await func())
                    break;
                if (stop.Elapsed > TimeSpan.FromSeconds(5))
                    throw new TimeoutException();
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }
        }

        private class DummyDiskStatsGetter : DiskStatsGetter<DummyDiskStatsRawResult>
        {
            private readonly ManualResetEvent _mre;
            private int _nextValue;


            public DummyDiskStatsGetter(TimeSpan minInterval, ManualResetEvent mre) : base(minInterval)
            {
                _mre = mre;
            }

            protected override DiskStatsResult CalculateStats(DummyDiskStatsRawResult currentInfo, State state)
            {
                _mre.WaitOne();
                return new DiskStatsResult { QueueLength = _nextValue++ };
            }

            protected override DummyDiskStatsRawResult GetDiskInfo(string path)
            {
                _mre.WaitOne();
                return new DummyDiskStatsRawResult { Time = DateTime.UtcNow };
            }

            public override void Dispose() { }
        }

        private class DummyDiskStatsRawResult : IDiskStatsRawResult
        {
            public DateTime Time { get; init; }
            public int Value { get; init; }
        }

        private static async Task DiskStats_WhenGetInParallel_ShouldTakeTheSameAsSequential(IDiskStatsGetter diskStatsGetter)
        {
            var currentDirectory = Directory.GetCurrentDirectory();

            var baseTime = TimeSpan.Zero;
            const int controlGroupCount = 10;
            for (int i = 0; i < controlGroupCount; i++)
            {
                var stop = Stopwatch.StartNew();
                await diskStatsGetter.GetAsync(currentDirectory);
                var stopElapsed = stop.Elapsed;
                baseTime += stopElapsed;
            }

            baseTime /= controlGroupCount;

            var errorCount = 0;
            const int taskCount = 100;
            const int iterationsCount = 100;

            var tasks = Enumerable.Range(0, taskCount).Select(i =>
                Task.Run(async () =>
                {
                    var stop = Stopwatch.StartNew();

                    for (int j = 0; j < iterationsCount; j++)
                    {
                        stop.Restart();
                        _ = await diskStatsGetter.GetAsync(currentDirectory);
                        var stopElapsed = stop.Elapsed;
                        if (10 * baseTime < stopElapsed)
                            Interlocked.Increment(ref errorCount);
                    }
                })).ToArray();

            await Task.WhenAll(tasks);
            var α = (double)errorCount / (taskCount * iterationsCount);
            Assert.True(α < 0.01, $"baseTime:{baseTime} errorCount:{errorCount} total:{taskCount * iterationsCount} α:{α}");
        }
    }
}
