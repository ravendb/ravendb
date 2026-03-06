using System;
using System.Collections.Generic;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16507 : StorageTest
    {
        public RavenDB_16507(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void AcquirePagePointerMustNotUseDisposedPagerState()
        {
            RequireFileBasedPager();

            using (var tx = Env.ReadTransaction())
            {
                var dataFilePager = tx.LowLevelTransaction.DataPager;

                dataFilePager.EnsureContinuous(1000, 1);

                var testingStuff = tx.LowLevelTransaction.ForTestingPurposesOnly();

                using (testingStuff.CallDuringEnsurePagerStateReference(() =>
                {
                    dataFilePager.EnsureContinuous(5000, 1);
                }))
                {
                    // under the covers this was using the pager state that has been disposed by above call dataFilePager.EnsureContinuous(5000, 1);
                    // now we have a check for that which throw an exception when this is the case

                    tx.LowLevelTransaction.DataPager.AcquirePagePointer(tx.LowLevelTransaction, 0);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MustReleaseAllReferencesToPagerState()
        {
            var tempPager = Env.Options.CreateTemporaryBufferPager($"temp-{Guid.NewGuid()}", 16 * 1024);

            var pagerStates = new HashSet<PagerState>();

            try
            {
                using (var readTx = Env.ReadTransaction())
                {
                    var state1 = tempPager.PagerState;

                    pagerStates.Add(state1);

                    tempPager.EnsureContinuous(1000, 1);

                    var state2 = tempPager.PagerState;

                    pagerStates.Add(state2);

                    tempPager.EnsureContinuous(3000, 1);

                    readTx.LowLevelTransaction.EnsurePagerStateReference(ref state1);
                    readTx.LowLevelTransaction.EnsurePagerStateReference(ref state2);
                }
            }
            finally
            {
                pagerStates.Add(tempPager.PagerState);

                tempPager.Dispose();
            }

            Assert.Equal(3, pagerStates.Count);

            foreach (var state in pagerStates)
            {
                Assert.True(state._released);
            }
        }
    }
}
