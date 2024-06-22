using System;
using System.Collections.Generic;
using FastTests.Voron;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16507 : StorageTest
    {
        public RavenDB_16507(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public unsafe void AcquirePagePointerMustNotUseDisposedPagerState()
        {
            RequireFileBasedPager();

            using (var tx = Env.ReadTransaction())
            {
                var dataFilePager = tx.LowLevelTransaction.DataPager;

                var testingStuff = tx.LowLevelTransaction.ForTestingPurposesOnly();

                using (testingStuff.CallDuringEnsurePagerStateReference(() =>
                {
                    dataFilePager.EnsureContinuous(ref tx.LowLevelTransaction.DataPagerState, 5000, 1, 0x0ff);
                }))
                {
                    // under the covers this was using the pager state that has been disposed by above call dataFilePager.EnsureContinuous(5000, 1);
                    // now we have a check for that which throw an exception when this is the case

                    tx.LowLevelTransaction.DataPager.AcquirePagePointer(tx.LowLevelTransaction.DataPagerState, ref tx.LowLevelTransaction.PagerTransactionState, 0);
                }
            }
        }

        [Fact]
        public void MustReleaseAllReferencesToPagerState()
        {
            var (tempPager, state) = Env.Options.CreateTemporaryBufferPager($"temp-{Guid.NewGuid()}", 16 * 1024, encrypted: false);

            var pagerStates = new HashSet<Pager2.State> { state };

            using(tempPager)
            {
                using (var readTx = Env.ReadTransaction())
                {
                    tempPager.EnsureContinuous(ref state, 1000, 1, 0x0ff);
                    pagerStates.Add(state);
                    tempPager.EnsureContinuous(ref state, 3000, 1, 0x0ff);
                    pagerStates.Add(state);
                }
            }

            Assert.Equal(3, pagerStates.Count);

            foreach (var s in pagerStates)
            {
                Assert.True(s.Disposed);
            }
        }
    }
}
