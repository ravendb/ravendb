using FastTests.Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12151 : StorageTest
    {
        [Fact]
        public void ShouldProperlyReportNumberOfModifiedPagesInTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;

                var numberOfModifiedPages = llt.NumberOfModifiedPages;

                tx.LowLevelTransaction.ModifyPage(0);

                Assert.Equal(1, llt.NumberOfModifiedPages - numberOfModifiedPages);
            }
        }
    }
}
