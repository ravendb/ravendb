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
        public async Task LinuxDiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet()
        {
#pragma warning disable CA1416
            var diskStatsGetter = new LinuxDiskStatsGetter(TimeSpan.FromMilliseconds(100));
#pragma warning restore CA1416
            await DiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet(diskStatsGetter);
        }
        
        [NightlyBuildMultiplatformFact(RavenPlatform.Windows, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-21945")]
        public async Task WindowsDiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet()
        {
#pragma warning disable CA1416
            var diskStatsGetter = new WindowsDiskStatsGetter(TimeSpan.FromMilliseconds(100));
#pragma warning restore CA1416
            await DiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet(diskStatsGetter);
        }

        private static async Task DiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet(IDiskStatsGetter diskStatsGetter)
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
            var α = (double)errorCount / taskCount * iterationsCount;
            Assert.True(α < 0.01, $"baseTime:{baseTime} errorCount:{errorCount} total:{taskCount * iterationsCount} α:{α}");
        }
    }
}
