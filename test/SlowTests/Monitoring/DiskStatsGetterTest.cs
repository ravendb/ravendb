using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server.Utils;
using Xunit;

namespace SlowTests.Monitoring
{
    public class DiskStatsGetterTest
    {
        [Fact]
        public async Task DiskStats_WhenGet_ShouldBeLessThenTwoSimpleGet()
        {
            var start = Stopwatch.StartNew();
            var currentDirectory = Directory.GetCurrentDirectory();
            var diskStatsGetter = new DiskStatsGetter(TimeSpan.FromMilliseconds(100));

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
            var tasks = Enumerable.Range(0, 100).Select(i =>
                Task.Run(async () =>
                {
                    var count = 0;
                    while(start.Elapsed < TimeSpan.FromSeconds(5))
                    {
                        var stop = Stopwatch.StartNew();
                        _ = await diskStatsGetter.GetAsync(currentDirectory);
                        var stopElapsed = stop.Elapsed;
                        if(2 * baseTime < stopElapsed)
                            Interlocked.Increment(ref errorCount);
                        count++;
                    }
                    return count;
                })).ToArray();

            await Task.WhenAll(tasks);
            var total = tasks.Sum(t => t.Result);
            var α = (double)errorCount / total;
            Assert.True(α < 0.01, $"baseTime:{baseTime} errorCount:{errorCount} total:{total} α:{α}");
        }
    }
}
