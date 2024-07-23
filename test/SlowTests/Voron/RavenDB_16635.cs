using System;
using System.Linq;
using System.Threading;
using FastTests.Voron;
using Sparrow.LowMemory;
using Voron;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_16635 : StorageTest
    {
        public RavenDB_16635(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxScratchBufferSize = 64 * 1024 * 4;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void MustNotThrowObjectDisposedOnScratchPagerWhenCreatingNewTransaction(bool startWriteTransaction)
        {
            RequireFileBasedPager();

            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("items");

                    tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            Exception startTransactionException = null;

            var startTransactionThread = new Thread(() =>
            {
                try
                {
                    using (var tx = startWriteTransaction ? Env.WriteTransaction() : Env.ReadTransaction())
                    {
                        var pagerStates = tx.LowLevelTransaction.CurrentStateRecord.ScratchPagesTable.Select(x => x.Value.State)
                            .Concat(new[] { tx.LowLevelTransaction.DataPagerState })
                            .ToHashSet();

                        Assert.Equal(2, pagerStates.Count); // data file, and one scratch file

                        foreach (var pagerState in pagerStates)
                        {
                            Assert.False(pagerState.Disposed);
                        }
                    }
                }
                catch (Exception e)
                {
                    startTransactionException = e;
                }
            });

            var startTransactionWasCalled = false;

            using (Env.ScratchBufferPool.ForTestingPurposesOnly().CallDuringCleanupRightAfterRemovingInactiveScratches(() =>
            {
                startTransactionWasCalled = true;

                startTransactionThread.Start();

                Thread.Sleep(1000); // give the thread starting new transaction more time
            }))
            {
                Env.ScratchBufferPool.Cleanup();
            }

            Assert.True(startTransactionWasCalled, "startTransactionWasCalled");

            Assert.True(startTransactionThread.Join(TimeSpan.FromSeconds(30)), "startTransactionThread.Join(TimeSpan.FromSeconds(30))");
            
            Assert.Null(startTransactionException);
        }

        [Fact]
        public void MustNotThrowObjectDisposedOnScratchPagerWhenCreatingNewReadTransactionRightAfterDisposingRecycledScratches()
        {
            RequireFileBasedPager();

            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("items");

                    tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("items");

                tree.Add("foo/0", new byte[] { 1, 2, 3 });

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("items");

                tree.Add("foo/1", new byte[] { 1, 2, 3 });

                tx.Commit();
            }

            Exception startTransactionException = null;

            var startTransactionThread = new Thread(() =>
            {
                try
                {
                    using (var tx = Env.ReadTransaction())
                    {
                        var pagerStates = tx.LowLevelTransaction.CurrentStateRecord.ScratchPagesTable.Select(x => x.Value.State)
                            .Concat(new[] { tx.LowLevelTransaction.DataPagerState })
                            .ToHashSet();
                        
                        Assert.Equal(2, pagerStates.Count); // data file, and one scratch file
                        
                        foreach (var pagerState in pagerStates)
                        {
                            Assert.False(pagerState.Disposed);
                        }
                    }
                }
                catch (Exception e)
                {
                    startTransactionException = e;
                }
            });

            var startTransactionWasCalled = false;

            using (Env.ScratchBufferPool.ForTestingPurposesOnly().CallDuringRemovalsOfRecycledScratchesRightAfterDisposingScratch(() =>
            {
                if (startTransactionWasCalled)
                    return;

                startTransactionWasCalled = true;

                startTransactionThread.Start();

                Thread.Sleep(1000); // give the thread starting new transaction more time
            }))
            {
                Env.ScratchBufferPool.RecycledScratchFileTimeout = TimeSpan.Zero; // by default we don't dispose scratches if they were recycled less than 1 minute ago

                Env.FlushLogToDataFile();

            }

            Assert.True(startTransactionWasCalled, "startTransactionWasCalled");

            Assert.True(startTransactionThread.Join(TimeSpan.FromSeconds(30)), "startTransactionThread.Join(TimeSpan.FromSeconds(30))");

            Assert.Null(startTransactionException);
        }

        [Fact]
        public unsafe void CannotUsePagerStateOfDisposedPager() // this test is just to verify that we properly throw on such invalid usage
        {
            RequireFileBasedPager();

            var (tempPager, state) = Env.Options.CreateTemporaryBufferPager($"temp-{Guid.NewGuid()}", 16 * 1024, encrypted: false);

            using var __ = state;
            tempPager.Dispose();

            Pager.PagerTransactionState txState = default;

            Assert.Throws<ObjectDisposedException>(() => tempPager.AcquirePagePointer(state, ref txState, 0));

            (tempPager, state) = Env.Options.CreateTemporaryBufferPager($"temp-{Guid.NewGuid()}", 16 * 1024, encrypted: false);
            using var _ = tempPager;
            state.Dispose();
            Assert.Throws<ObjectDisposedException>(() => tempPager.AcquirePagePointer(state, ref txState, 0));

        }


        [Fact]
        public void MustNotThrowObjectDisposedOnScratchPagerWhenCreatingNewReadTransaction()
        {
            RequireFileBasedPager();

            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("items");

                    tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            Env.ScratchBufferPool.LowMemory(LowMemorySeverity.Low); // we're simulating low memory so then the scratches aren't recycled - in that case we won't move then to recycle area so RemoveInactiveScratches disposes them immediately 

            Env.FlushLogToDataFile();

            var testActionCalled = false;

            using (Env.ScratchBufferPool.ForTestingPurposesOnly().CallDuringRemovalsOfInactiveScratchesRightAfterDisposingScratch(() =>
            {
                testActionCalled = true;

                using (Env.ReadTransaction())
                {

                }
            }))
            {
                for (int i = 0; i < 100; i++)
                {
                    using (var tx = Env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("items");

                        tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                        tx.Commit();
                    }
                }
            }

            Assert.True(testActionCalled, "testActionCalled");
        }
    }
}
