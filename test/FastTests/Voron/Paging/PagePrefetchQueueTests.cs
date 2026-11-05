using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Xunit;
using static FastTests.Voron.Paging.PrefetchTestData;

namespace FastTests.Voron.Paging
{
    // The pager only queues requests for the prefetching thread: the queue state must not change what is read.
    public unsafe class PagePrefetchQueueTests(ITestOutputHelper output) : StorageTest(output)
    {
        private const long SegmentSize = 4 * 1024 * 1024;
        private const int Items = 12_000; // about 12 MB: the data file is 16 MB, 4 segments (the scanning test needs at least 4)

        private void WriteAndRestart()
        {
            RequireFileBasedPager();
            Write(Env, "data", "items", 0, Items);
            RestartDatabase();
        }

        private PalDefinitions.PrefetchRanges FirstSegmentCommand()
        {
            var state = Env.Options.DataPager.PagerState;
            var allocation = state.AllocationInfos[0];
            return new PalDefinitions.PrefetchRanges(allocation.BaseAddress, Math.Min(allocation.Size, SegmentSize)); // the size of what the pager asks for
        }

        private static bool WaitForQueueToDrain()
        {
            var queue = GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue;
            var sw = Stopwatch.StartNew();
            while (queue.Count != 0)
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(30))
                    return false;
                Thread.Sleep(10);
            }

            return true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ScanningHandsTheSegmentsToThePrefetchingThread()
        {
            WriteAndRestart();
            var pager = Env.Options.DataPager;
            var allocation = pager.PagerState.AllocationInfos[0];
            var state = new PagerState(pager, SegmentSize, Env.Options.PrefetchResetThreshold, allocation); // nothing is prefetched yet, and it is not released: the pager owns the mapping
            var segments = allocation.Size / SegmentSize;
            Assert.True(segments >= 4, "The data file is expected to hold several segments, it holds " + segments);

            var prefetcher = GlobalPrefetchingBehavior.GlobalPrefetcher.Value;
            Assert.True(WaitForQueueToDrain());
            var before = prefetcher.RequestsQueued + prefetcher.RequestsDropped;

            // sequential scan: each segment is requested once, through the prefetching thread
            for (var page = 0L; page < pager.NumberOfAllocatedPages; page++)
                pager.AcquirePagePointer(null, page, state);

            var requests = prefetcher.RequestsQueued + prefetcher.RequestsDropped - before;
            Output.WriteLine("Segments: " + segments + ", requests handed to the prefetching thread: " + requests);
            Assert.True(requests >= segments, "Expected " + segments + " segments to be handed to the prefetching thread, got " + requests);
            Assert.True(WaitForQueueToDrain());
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingWhileTheQueueIsFullGivesTheDataThatWasWritten()
        {
            WriteAndRestart();
            var command = FirstSegmentCommand();
            var queue = GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue;

            using var stop = new CancellationTokenSource();
            long refused = 0;
            var flooders = new Task[4];
            for (var t = 0; t < flooders.Length; t++)
            {
                flooders[t] = Task.Factory.StartNew(() =>
                {
                    while (stop.IsCancellationRequested == false)
                    {
                        if (queue.TryAdd(command, 0) == false)
                            Interlocked.Increment(ref refused);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            try
            {
                // a scan and then random reads
                VerifyRange(Env, "data", "items", 0, Items);
                using var tx = Env.ReadTransaction();
                var tree = tx.ReadTree("data");
                var random = new Random(3);
                for (var n = 0; n < Items; n++)
                    Verify(tree, "items", random.Next(Items));
            }
            finally
            {
                stop.Cancel();
                Task.WaitAll(flooders);
            }

            Output.WriteLine("Requests refused because the queue was full: " + Interlocked.Read(ref refused));
            Assert.True(WaitForQueueToDrain(), "The prefetching thread does not take the requests from the queue");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ARefusedRequestDoesNotStopThePrefetcherFromWorkingAfterwards()
        {
            WriteAndRestart();
            var command = FirstSegmentCommand();
            var queue = GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue;

            for (var i = 0; i < 100_000; i++)
                queue.TryAdd(command, 0); // most of them are refused

            Assert.True(WaitForQueueToDrain());
            Assert.True(queue.TryAdd(command, 0));
            Assert.True(WaitForQueueToDrain());

            VerifyRange(Env, "data", "items", 0, Items);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ThePagerCanBeDisposedWhileItsRequestsAreQueued()
        {
            WriteAndRestart();
            var command = FirstSegmentCommand();
            var queue = GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue;

            for (var i = 0; i < 128; i++)
                queue.TryAdd(command, 0);

            RestartDatabase(); // the old pager is unmapped while the prefetching thread has requests for its memory

            Assert.True(WaitForQueueToDrain(), "The prefetching thread does not take the requests from the queue");

            // the prefetching thread is still working
            Assert.True(queue.TryAdd(FirstSegmentCommand(), 0));
            Assert.True(WaitForQueueToDrain());
            VerifyRange(Env, "data", "items", 0, Items);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ThePagerCanBeDisposedAndReopenedAgainAndAgainWhileTheQueueIsBusy()
        {
            WriteAndRestart();
            var queue = GlobalPrefetchingBehavior.GlobalPrefetcher.Value.CommandQueue;

            for (var round = 0; round < 5; round++)
            {
                var command = FirstSegmentCommand();
                for (var i = 0; i < 128; i++)
                    queue.TryAdd(command, 0);

                RestartDatabase();

                // reads on the new pager, which may ask for the same addresses as the old one
                VerifyRange(Env, "data", "items", round * 1000, 5000);
            }

            Assert.True(WaitForQueueToDrain());
            VerifyRange(Env, "data", "items", 0, Items);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TwoPagersReadingAtTheSameTimeGetTheirOwnData()
        {
            RequireFileBasedPager();
            Write(Env, "data", "first", 0, Items);

            var secondPath = RavenTestHelper.NewDataPath(nameof(TwoPagersReadingAtTheSameTimeGetTheirOwnData) + "Second", 0);
            IOExtensions.DeleteDirectory(secondPath);
            try
            {
                using var secondOptions = StorageEnvironmentOptions.ForPathForTests(secondPath);
                using var second = new StorageEnvironment(secondOptions);
                Write(second, "data", "second", 0, Items / 2);

                Assert.NotSame(Env.Options.DataPager, second.Options.DataPager);

                var readers = new List<Task>();
                for (var t = 0; t < 4; t++)
                {
                    var seed = t;
                    readers.Add(Task.Run(() =>
                    {
                        // both pagers queue requests for the one prefetcher
                        var env = seed % 2 == 0 ? Env : second;
                        var prefix = seed % 2 == 0 ? "first" : "second";
                        var count = seed % 2 == 0 ? Items : Items / 2;
                        using var tx = env.ReadTransaction();
                        var tree = tx.ReadTree("data");
                        for (var i = 0; i < count; i++)
                            Verify(tree, prefix, i);
                        var random = new Random(seed);
                        for (var i = 0; i < count; i++)
                            Verify(tree, prefix, random.Next(count));
                    }));
                }

                Task.WaitAll(readers.ToArray());
            }
            finally
            {
                IOExtensions.DeleteDirectory(secondPath);
            }
        }

        // grows the data file, adds some items, returns how many
        private int GrowTheFile(int firstItem, long sizeBefore)
        {
            const int added = 1000;
            var pager = Env.Options.DataPager;

            var newState = pager.EnsureContinuous(pager.NumberOfAllocatedPages, 1);
            Assert.NotNull(newState);
            Assert.True(pager.PagerState.AllocationInfos[0].Size > sizeBefore, "The file was not grown");

            Write(Env, "data", "items", firstItem, added);
            return added;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingBackwardsFromTheEndOfTheFileGivesTheDataThatWasWritten()
        {
            WriteAndRestart();

            using var tx = Env.ReadTransaction();
            var tree = tx.ReadTree("data");
            for (var i = Items - 1; i >= 0; i--)
                Verify(tree, "items", i);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingTheTailOfTheFileFirstAfterARestartGivesTheDataThatWasWritten()
        {
            // the last segment is partial
            WriteAndRestart();

            using var tx = Env.ReadTransaction();
            var tree = tx.ReadTree("data");
            for (var n = 0; n < 50; n++)
            {
                Verify(tree, "items", Items - 1);
                Verify(tree, "items", Items - 2 - n);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingAfterSeveralRestartsGivesTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write(Env, "data", "items", 0, Items / 2);

            for (var round = 0; round < 3; round++)
            {
                RestartDatabase();
                VerifyRange(Env, "data", "items", 0, Items / 2 + round * 1000);
                Write(Env, "data", "items", Items / 2 + round * 1000, 1000);
            }

            RestartDatabase();
            VerifyRange(Env, "data", "items", 0, Items / 2 + 3000);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadingAFileThatWasGrownGivesTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write(Env, "data", "items", 0, Items / 2);
            RestartDatabase();

            var sizeBefore = Env.Options.DataPager.PagerState.AllocationInfos[0].Size;

            // scan what there is: the prefetched segments are copied to the state of the grown file
            VerifyRange(Env, "data", "items", 0, Items / 2);

            var written = GrowTheFile(Items / 2, sizeBefore);

            VerifyRange(Env, "data", "items", 0, Items / 2 + written);
            VerifyRange(Env, "data", "items", Items / 2, written);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AReaderThatStartedBeforeTheFileWasGrownKeepsReadingTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write(Env, "data", "items", 0, Items / 2);
            RestartDatabase();

            var sizeBefore = Env.Options.DataPager.PagerState.AllocationInfos[0].Size;

            using var tx = Env.ReadTransaction(); // holds the pager state of the file before it is grown
            var tree = tx.ReadTree("data");
            for (var i = 0; i < Items / 4; i++)
                Verify(tree, "items", i);

            var written = GrowTheFile(Items / 2, sizeBefore);

            for (var i = 0; i < Items / 2; i++)
                Verify(tree, "items", i);

            VerifyRange(Env, "data", "items", 0, Items / 2 + written);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ReadersWhileAWriterIsAddingDataGetTheDataThatWasWritten()
        {
            RequireFileBasedPager();
            Write(Env, "data", "items", 0, Items);
            RestartDatabase();

            const int batches = 20;
            const int batchSize = 1000;
            using var done = new CancellationTokenSource();

            var writer = Task.Run(() =>
            {
                try
                {
                    // the file grows while the readers prefetch it
                    for (var b = 0; b < batches; b++)
                        Write(Env, "extra", "extra", b * batchSize, batchSize);
                }
                finally
                {
                    done.Cancel();
                }
            });

            var readers = new Task[5];
            for (var t = 0; t < readers.Length; t++)
            {
                var seed = t;
                readers[t] = Task.Run(() =>
                {
                    var random = new Random(seed);
                    do
                    {
                        using var tx = Env.ReadTransaction();
                        var tree = tx.ReadTree("data");
                        if (seed % 2 == 0)
                        {
                            var start = random.Next(Items);
                            for (var n = 0; n < 5000; n++)
                                Verify(tree, "items", (start + n) % Items);
                        }
                        else
                        {
                            for (var n = 0; n < 5000; n++)
                                Verify(tree, "items", random.Next(Items));
                        }
                    } while (done.IsCancellationRequested == false);
                });
            }

            Task.WaitAll(readers);
            writer.GetAwaiter().GetResult();

            VerifyRange(Env, "data", "items", 0, Items);
            VerifyRange(Env, "extra", "extra", 0, batches * batchSize);

            RestartDatabase();
            VerifyRange(Env, "data", "items", 0, Items);
            VerifyRange(Env, "extra", "extra", 0, batches * batchSize);
        }
    }
}
