using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Indexing;
using RTP = Raven.Database.Impl.BackgroundTaskExecuter.RavenThreadPool;
using Raven.Tests.Common;
using Xunit;

namespace Raven.SlowTests.RavenThreadPool
{
    public class ParallelCalculation : RavenTest
    {
        [Fact]
        public void OneLevelParallelCalculation()
        {

            using (var tp = new RTP(8))
            {
                long sum = 0;
                tp.Start();
                var range = Enumerable.Range(0, 100000).ToList();
                tp.ExecuteBatch(range, (int input) =>
                {
                    Interlocked.Add(ref sum, (long)input);
                },null);
                var expectedSum = range.Sum(x => (long)x);
                Assert.Equal(expectedSum, sum);
            }
        }

        [Fact]
        public void OneLevelParallelBulkCalculationSimple()
        {
            using (var tp = new RTP(8))
            {
                long sum = 0;
                tp.Start();
                var range = Enumerable.Range(0, 100000).ToList();

                tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                {
                    while (input.MoveNext())
                    {
                        Interlocked.Add(ref sum, (long)input.Current);
                    }
                },null);


                var expectedSum = range.Sum(x => (long)x);
                Assert.Equal(expectedSum, sum);
            }
        }

        [Fact]
        public void OneLevelParallelBulkCalculation()
        {
            using (var tp = new RTP(8))
            {
                long sum = 0;
                tp.Start();
                var range = Enumerable.Range(0, 100000).ToList();
                var expectedSum = range.Sum(x => (long)x);
                for (int i = 1; i <= 1024; i++)
                {
                    var sp = Stopwatch.StartNew();
                    tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                    {
                        while (input.MoveNext())
                        {
                            Interlocked.Add(ref sum, (long)input.Current);
                        }

                    }, pageSize:i, database:null);

                    Assert.Equal(expectedSum, sum);
                    sum = 0;
                }
            }
        }


        [Fact]
        public void ThrottlingTest()
        {
            using (var tp = new RTP(8).Start())
            {
                var threadPoolStats = tp.GetThreadPoolStats();

                Assert.Equal(8, threadPoolStats.ThreadsPrioritiesCounts[ThreadPriority.Normal]);
                Assert.Equal(8, threadPoolStats.ConcurrentWorkingThreadsAmount);

                for (var i = 0; i < 8; i++)
                {
                    tp.HandleHighCpuUsage();
                    int normalPrioritiesCount = 0, belowNormalPrioritiesCount = 0;
                    threadPoolStats = tp.GetThreadPoolStats();
                    var threadPrioritiesCounts = threadPoolStats.ThreadsPrioritiesCounts;
                    threadPrioritiesCounts.TryGetValue(ThreadPriority.Normal, out normalPrioritiesCount);
                    threadPrioritiesCounts.TryGetValue(ThreadPriority.BelowNormal, out belowNormalPrioritiesCount);

                    Assert.Equal(7 - i, normalPrioritiesCount);
                    Assert.Equal(i + 1, belowNormalPrioritiesCount);
                    Assert.Equal(8, threadPoolStats.ConcurrentWorkingThreadsAmount);
                }

                threadPoolStats = tp.GetThreadPoolStats();
                Assert.Equal(8, threadPoolStats.ConcurrentWorkingThreadsAmount);

                for (var i = 0; i < 7; i++)
                {
                    tp.HandleHighCpuUsage();
                    threadPoolStats = tp.GetThreadPoolStats();

                    Assert.Equal(7 - i, threadPoolStats.ConcurrentWorkingThreadsAmount);
                }

                for (var i = 1; i < 8; i++)
                {
                    tp.HandleLowCpuUsage();
                    threadPoolStats = tp.GetThreadPoolStats();

                    Assert.Equal(i + 1, threadPoolStats.ConcurrentWorkingThreadsAmount);
                }

                for (var i = 1; i <= 8; i++)
                {
                    tp.HandleLowCpuUsage();
                    int normalPrioritiesCount = 0, belowNormalPrioritiesCount = 0;
                    threadPoolStats = tp.GetThreadPoolStats();
                    var threadPrioritiesCounts = threadPoolStats.ThreadsPrioritiesCounts;
                    threadPrioritiesCounts.TryGetValue(ThreadPriority.Normal, out normalPrioritiesCount);
                    threadPrioritiesCounts.TryGetValue(ThreadPriority.BelowNormal, out belowNormalPrioritiesCount);

                    Assert.Equal(i, normalPrioritiesCount);
                    Assert.Equal(8 - i, belowNormalPrioritiesCount);
                    Assert.Equal(8, threadPoolStats.ConcurrentWorkingThreadsAmount);
                }
            }
        }

        [Fact]
        public void PartialMaxWaitThrottlingTest()
        {
            using (var tp = new RTP(8).Start())
            {
                var array = Enumerable.Range(0, 6).ToList();
                var stats = tp.GetThreadPoolStats();

                Debug.Print(stats.PartialMaxWait.ToString());
                for (var i = 0; i < 8; i++)
                {
                    tp.ExecuteBatch(array, (int x) =>
                    {
                        Thread.Sleep(x * 1000);
                    }, allowPartialBatchResumption: true,database:null);
                    stats = tp.GetThreadPoolStats();

                    Debug.Print(stats.PartialMaxWait.ToString());
                }
            }
        }

        [Fact]
        public void OneLevelParallelCalculationWithPartialResumption()
        {
            using (var tp = new RTP(8).Start())
            {
                long sum = 0;
                var range = Enumerable.Range(1, 6).ToList();
                tp.ExecuteBatch(range, (int input) =>
                {
                    Interlocked.Add(ref sum, (long)input);
                    Thread.Sleep((int)Math.Pow(input, 4) * 5);
                }, allowPartialBatchResumption: true,database:null);
                
                while (tp.GetAllWaitingTasks().Count() != 0 || tp.RunningTasksAmount != 0)
                {
                    Thread.Sleep(100);
                }
                var expectedSum = range.Sum(x => (long)x);
                Console.WriteLine($"expected:{expectedSum}; real:{sum}");
                Assert.Equal(expectedSum, sum);
            }
        }

        [Fact]
        public void TwoLevelParallelCalculation()
        {
            using (var tp = new RTP(8))
            {
                long sum = 0;
                tp.Start();
                var range = Enumerable.Range(0, 1000).ToList();
                tp.ExecuteBatch(range, (int input) =>
                {
                    var innerRange = Enumerable.Range(0, 100).ToList();
                    tp.ExecuteBatch(innerRange, (int innerInput) => Interlocked.Add(ref sum, (long)input),database:null);

                },database:null);
                var expectedSum = range.Sum(x => (long)x);
                Assert.Equal(100 * expectedSum, sum);
            }
        }

        [Fact]
        public void TwoLevelParallelBulkCalculationSimple()
        {
            using (var tp = new RTP(8))
            {
                long sum = 0;
                tp.Start();
                var range = Enumerable.Range(0, 100000).ToList();


                tp.ExecuteBatch(range, (IEnumerator<int> input) =>
                {
                    var inputAsList = new List<int>();

                    while (input.MoveNext())
                    {
                        inputAsList.Add(input.Current);
                    }
                    tp.ExecuteBatch(inputAsList, x => Interlocked.Add(ref sum, (long)x),database:null);
                },database:null);

                var expectedSum = range.Sum(x => (long)x);
                Assert.Equal(expectedSum, sum);
            }
        }
    }
}
