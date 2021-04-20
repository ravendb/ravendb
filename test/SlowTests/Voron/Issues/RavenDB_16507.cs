using FastTests.Voron;
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
    }
}
