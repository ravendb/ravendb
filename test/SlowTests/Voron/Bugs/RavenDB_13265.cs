using FastTests.Voron;
using Voron;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_13265 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void EnsurePagerStateReferenceMustAdd_Current_PagerStateToCollectionSoWeWillReleaseItsReferenceOnTxDispose()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                var dataFilePager = tx.LowLevelTransaction.DataPager;

                dataFilePager.EnsureContinuous(1000, 1);

                var testingStuff = tx.LowLevelTransaction.ForTestingPurposesOnly();
                
                using (testingStuff.CallDuringEnsurePagerStateReference(() =>
                {
                    dataFilePager.EnsureContinuous(5000, 1);
                }))
                {
                    tx.LowLevelTransaction.EnsurePagerStateReference(dataFilePager.PagerState);

                    Assert.Contains(dataFilePager.PagerState, testingStuff.GetPagerStates());
                }
            }
        }
    }
}
