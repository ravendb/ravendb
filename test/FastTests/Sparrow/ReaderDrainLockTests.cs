using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class ReaderDrainLockTests : NoDisposalNeeded
    {
        public ReaderDrainLockTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void TryEnterRead_GrantsWhenIdle()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle h));
            h.Dispose();
        }

        [RavenFact(RavenTestCategory.Core)]
        public void TryEnterRead_ManyReadersConcurrently()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            ReaderDrainLock.ReadHandle h1, h2, h3;
            Assert.True(l.TryEnterRead(out h1));
            Assert.True(l.TryEnterRead(out h2));
            Assert.True(l.TryEnterRead(out h3));

            h1.Dispose();
            h2.Dispose();
            h3.Dispose();
        }

        [RavenFact(RavenTestCategory.Core)]
        public void TryEnterRead_FailsWhileWriterPending()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            // Hold a reader so the writer must wait.
            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle reader));

            ManualResetEventSlim writerPending = new ManualResetEventSlim();
            Task writerTask = Task.Run(() =>
            {
                using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                // Set the flag *before* entering the wait by doing a tiny pre-step:
                // EnterWrite publishes the pending bit before waiting on drain.
                writerPending.Set();
                using IDisposable _ = l.EnterWrite(cts.Token);
            });

            // Give the writer a moment to publish the pending bit.
            writerPending.Wait();
            SpinWait.SpinUntil(() =>
            {
                if (l.TryEnterRead(out ReaderDrainLock.ReadHandle h) == false)
                    return true;
                h.Dispose();
                return false;
            }, TimeSpan.FromSeconds(1));

            // While the writer is pending, new readers must be blocked.
            Assert.False(l.TryEnterRead(out ReaderDrainLock.ReadHandle blocked));

            reader.Dispose();
            writerTask.Wait(TimeSpan.FromSeconds(5));
            Assert.True(writerTask.IsCompletedSuccessfully);

            // Once the writer is gone, readers proceed.
            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle after));
            after.Dispose();
        }


        [RavenFact(RavenTestCategory.Core)]
        public void EnterWrite_WaitsForActiveReaderThenAcquires()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle reader));

            ManualResetEventSlim writerEntered = new ManualResetEventSlim();
            Task writerTask = Task.Run(() =>
            {
                using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                using IDisposable _ = l.EnterWrite(cts.Token);
                writerEntered.Set();
            });

            // Writer should not enter while reader holds.
            Assert.False(writerEntered.Wait(TimeSpan.FromMilliseconds(150)));

            reader.Dispose();
            Assert.True(writerEntered.Wait(TimeSpan.FromSeconds(5)));
            writerTask.Wait(TimeSpan.FromSeconds(5));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EnterWrite_NoActiveReaders_AcquiresImmediately()
        {
            using ReaderDrainLock l = new ReaderDrainLock();
            using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            using IDisposable w = l.EnterWrite(cts.Token);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EnterWrite_TimeoutRollsBackPendingBit()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle reader));

            using (CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100)))
            {
                Assert.Throws<OperationCanceledException>(() => l.EnterWrite(cts.Token));
            }

            reader.Dispose();

            // After rollback, readers can proceed and a fresh writer can enter.
            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle after));
            after.Dispose();

            using CancellationTokenSource cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using IDisposable w = l.EnterWrite(cts2.Token);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EnterRead_HonorsCancellation()
        {
            using ReaderDrainLock l = new ReaderDrainLock();
            using CancellationTokenSource writerToken = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            Assert.True(l.TryEnterRead(out ReaderDrainLock.ReadHandle longReader));

            // Background writer that publishes the pending bit and then blocks
            // on the drain. While it is blocked, a fresh reader call must
            // wait, and we cancel that wait.
            ManualResetEventSlim writerPublishedPending = new ManualResetEventSlim();
            Task writerTask = Task.Run(() =>
            {
                writerPublishedPending.Set();
                using IDisposable _ = l.EnterWrite(writerToken.Token);
            });

            writerPublishedPending.Wait();
            // Spin briefly so EnterWrite has flipped the bit.
            SpinWait.SpinUntil(() =>
            {
                if (l.TryEnterRead(out ReaderDrainLock.ReadHandle probe) == false)
                    return true;
                probe.Dispose();
                return false;
            }, TimeSpan.FromSeconds(1));

            using CancellationTokenSource readerToken = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            Assert.Throws<OperationCanceledException>(() => l.EnterRead(readerToken.Token));

            longReader.Dispose();
            writerTask.Wait(TimeSpan.FromSeconds(5));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Readers_AndWriters_NeverOverlap()
        {
            using ReaderDrainLock l = new ReaderDrainLock();

            int activeReaders = 0;
            int activeWriters = 0;
            int violations = 0;

            using CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            int readerThreads = Math.Max(2, Environment.ProcessorCount);
            Task[] tasks = new Task[readerThreads + 1];

            for (int i = 0; i < readerThreads; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    while (!stop.IsCancellationRequested)
                    {
                        try
                        {
                            using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
                            using ReaderDrainLock.ReadHandle h = l.EnterRead(cts.Token);
                            Interlocked.Increment(ref activeReaders);
                            if (Volatile.Read(ref activeWriters) != 0)
                                Interlocked.Increment(ref violations);
                            Interlocked.Decrement(ref activeReaders);
                        }
                        catch (OperationCanceledException)
                        {
                        }
                    }
                });
            }

            tasks[readerThreads] = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
                        using IDisposable w = l.EnterWrite(cts.Token);
                        Interlocked.Increment(ref activeWriters);
                        if (Volatile.Read(ref activeReaders) != 0)
                            Interlocked.Increment(ref violations);
                        Thread.SpinWait(50);
                        Interlocked.Decrement(ref activeWriters);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            });

            Task.WaitAll(tasks);
            Assert.Equal(0, violations);
        }
    }
}
