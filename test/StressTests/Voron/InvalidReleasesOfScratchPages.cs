using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron
{
    public class InvalidReleasesOfScratchPages : StorageTest
    {
        public InvalidReleasesOfScratchPages(ITestOutputHelper output) : base(output)
        {
        }


        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize *= 2;
        }

        [Fact]
        public void ParallelWritesInBatchesAndReadsByUsingTreeIterator()
        {
            const int numberOfWriteThreads = 10;
            const int numberOfReadThreads = 10;
            const int numberOfTrees = 2;

            var trees = CreateTrees(Env, numberOfTrees, "tree");

            Task readParallelTask = null;

            var taskWorkTime = TimeSpan.FromSeconds(60);

            var writeTime = Stopwatch.StartNew();

            var writeParallelTask = Task.Factory.StartNew(
                () =>
                {
                    Parallel.For(
                        0,
                        numberOfWriteThreads,
                        RavenTestHelper.DefaultParallelOptions,
                        i =>
                        {
                            var random = new Random(i ^ 1337);
                            var dataSize = random.Next(100, 100);
                            var buffer = new byte[dataSize];
                            random.NextBytes(buffer);

                            while (writeTime.Elapsed < taskWorkTime && (readParallelTask == null || readParallelTask.Exception == null))
                            {
                                var tIndex = random.Next(0, numberOfTrees - 1);
                                var treeName = trees[tIndex];

                                using (var tx = Env.WriteTransaction(timeout: TimeSpan.FromMinutes(5)))
                                {
                                    var tree = tx.CreateTree(treeName);
                                    tree.Add("testdocuments/" + random.Next(0, 100000), new MemoryStream(buffer));
                                    tx.Commit();
                                }

                            }
                        });
                },
                TaskCreationOptions.LongRunning);

            var readTime = Stopwatch.StartNew();
            readParallelTask = Task.Factory.StartNew(
                () =>
                    {
                        Parallel.For(
                            0,
                            numberOfReadThreads,
                            RavenTestHelper.DefaultParallelOptions,
                            i =>
                                {
                                    var random = new Random(i);

                                    while (readTime.Elapsed < taskWorkTime)
                                    {
                                        var tIndex = random.Next(0, numberOfTrees - 1);
                                        var treeName = trees[tIndex];

                                        using (var snapshot = Env.ReadTransaction())
                                        using (var iterator = snapshot.ReadTree(treeName).Iterate(false))
                                        {
                                            if (!iterator.Seek(Slices.BeforeAllKeys))
                                            {
                                                continue;
                                            }

                                            do
                                            {
                                                Assert.Contains("testdocuments/", iterator.CurrentKey.ToString());
                                            } while (iterator.MoveNext());
                                        }
                                    }
                                });
                    },
                TaskCreationOptions.LongRunning);


            try
            {
                Task.WaitAll(new[] { writeParallelTask, readParallelTask });
            }
            catch (Exception ex)
            {
                var aggregate = ex as AggregateException;

                if (aggregate != null)
                {
                    foreach (var innerEx in aggregate.InnerExceptions)
                    {
                        Console.WriteLine(innerEx);
                    }
                }

                throw;
            }
        }
    }
}
