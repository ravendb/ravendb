using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Utils.IoMetrics;
using Sparrow.Server.Meters;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class IoMetric : NoDisposalNeeded
    {
        public IoMetric(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanProperlyReportIoMetrics()
        {
            var metrics = new IoMetrics(4, 4);

            for (var i = 0; i < 6; i++)
            {
                var now = DateTime.UtcNow;
                var meterIoRate = metrics.MeterIoRate("file1.txt", IoMetrics.MeterType.JournalWrite, i + 1);
                var durationMeasurement = new IoMeterBuffer.DurationMeasurement(meterIoRate.Parent, IoMetrics.MeterType.JournalWrite, i + 1, 0, null)
                {
                    Start = now,
                    End = now.AddMilliseconds(2)
                };
                meterIoRate.Parent.Mark(ref durationMeasurement);
            }

            int filesCount = 0;

            foreach (var file in metrics.Files)
            {
                filesCount++;
                var currentItems = new List<IoMeterBuffer.MeterItem>(file.JournalWrite.GetCurrentItems());
                var historyItems = new List<IoMeterBuffer.SummerizedItem>(file.JournalWrite.GetSummerizedItems());

                foreach (var currentItem in currentItems)
                {
                    Assert.InRange(currentItem.Duration.TotalMilliseconds, 0, 2);
                }

                Assert.Equal(1, historyItems.Count);
                Assert.Equal(1, currentItems.Count);

                Assert.Equal(1 + 2 + 3 + 4 + 5, historyItems[0].TotalSize);
                Assert.Equal(6L, currentItems[0].Size);
            }

            Assert.Equal(1, filesCount);

        }

        [Fact]
        public void CanReportMetricsInParallel()
        {
            var currentBuffer = 2048;
            var historicalBuffer = 20000;
            var metrics = new IoMetrics(currentBuffer, historicalBuffer);
            var forIterations = 1000 * 1000;
            Parallel.For(0, forIterations, RavenTestHelper.DefaultParallelOptions, i =>
            {
                using (metrics.MeterIoRate("file1.txt", IoMetrics.MeterType.JournalWrite, 1))
                {
                    // empty
                }
            });

            foreach (var file in metrics.Files)
            {
                var currentItems = new List<IoMeterBuffer.MeterItem>(file.JournalWrite.GetCurrentItems());
                var historyItems = new List<IoMeterBuffer.SummerizedItem>(file.JournalWrite.GetSummerizedItems());

                var totalItems = historyItems.Sum(x => x.Count) + currentItems.Count;

                Assert.Equal(forIterations, totalItems);

            }
        }

        [Fact]
        public void CanCleanExpiredMetrics()
        {
            var files = new List<string>(new[] { "file1.txt", "file2.txt", "file3.txt" });

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()))
            {
                var envWithType = new StorageEnvironmentWithType("test", StorageEnvironmentWithType.StorageEnvironmentType.Documents, env);

                foreach (var fileName in files)
                {
                    for (var i = 0; i < 2 * 256; i++)
                    {
                        var now = DateTime.UtcNow.AddHours(-7);
                        var meterIoRate = env.Options.IoMetrics.MeterIoRate(fileName, IoMetrics.MeterType.JournalWrite, i + 1);
                        var durationMeasurement = new IoMeterBuffer.DurationMeasurement(meterIoRate.Parent, IoMetrics.MeterType.JournalWrite, i + 1, 0, null)
                        {
                            Start = now,
                            End = now.AddMilliseconds(2)
                        };
                        meterIoRate.Parent.Mark(ref durationMeasurement);
                    }
                }

                CheckMetricFiles(env, files, checkForNull: false);

                IEnumerable<StorageEnvironmentWithType> environments = new[] { envWithType };
                IoMetricsUtil.CleanIoMetrics(environments, DateTime.UtcNow.Ticks);

                CheckMetricFiles(env, files, checkForNull: true);
            }
        }

        private void CheckMetricFiles(StorageEnvironment env, List<string> files, bool checkForNull)
        {
            foreach (var fileName in files)
            {
                var file = env.Options.IoMetrics.Files.First(x => x.FileName == fileName);
                var items = file.JournalWrite.GetCurrentItems().ToList();
                var summarizedItems = file.JournalWrite.GetSummerizedItems().ToList();

                if (checkForNull == false)
                {
                    foreach (var item in items)
                    {
                        Assert.NotNull(item);
                    }

                    foreach (var item in summarizedItems)
                    {
                        Assert.NotNull(item);
                    }
                }
                else
                {
                    Assert.Equal(0, items.Count);
                    Assert.Equal(0, summarizedItems.Count);
                }
            }
        }
    }
}
