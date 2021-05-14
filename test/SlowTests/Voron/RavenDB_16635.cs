using System;
using System.Threading;
using FastTests.Voron;
using Sparrow.LowMemory;
using Voron;
using Voron.Impl;
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
                        var pagerStates = tx.LowLevelTransaction.ForTestingPurposesOnly().GetPagerStates();

                        Assert.Equal(2, pagerStates.Count); // data file, and one scratch file

                        foreach (PagerState pagerState in pagerStates)
                        {
                            Assert.False(pagerState.CurrentPager.Disposed);
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
            }))
            {
                Env.ScratchBufferPool.Cleanup();
            }

            Assert.True(startTransactionWasCalled, "startTransactionWasCalled");

            Assert.True(startTransactionThread.Join(TimeSpan.FromSeconds(30)), "startTransactionThread.Join(TimeSpan.FromSeconds(30))");
            
            Assert.Null(startTransactionException);
        }

        [Fact]
        public unsafe void CannotUsePagerStateOfDisposedPager() // this test is just to verify that we properly throw on such invalid usage
        {
            RequireFileBasedPager();

            var tempPager = Env.Options.CreateTemporaryBufferPager($"temp-{Guid.NewGuid()}", 16 * 1024);

            var state = tempPager.PagerState;

            state.AddRef();

            tempPager.Dispose();

            using (var tx = Env.ReadTransaction())
            {
                Assert.Throws<ObjectDisposedException>(() => tempPager.AcquirePagePointer(tx.LowLevelTransaction, 0, state));
            }

            using (var tx = Env.ReadTransaction())
            {
                Env.Options.DataPager.Dispose();

                Assert.Throws<ObjectDisposedException>(() => Env.Options.DataPager.AcquirePagePointer(tx.LowLevelTransaction, 0));
            }
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
