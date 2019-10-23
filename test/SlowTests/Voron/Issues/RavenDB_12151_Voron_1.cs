using FastTests.Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12151_Voron_1 : StorageTest
    {
        public RavenDB_12151_Voron_1(ITestOutputHelper output) : base(output)
        {
        }

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
