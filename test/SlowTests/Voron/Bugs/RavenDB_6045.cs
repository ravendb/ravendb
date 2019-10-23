using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class RavenDB_6045 : FastTests.Voron.StorageTest
    {
        public RavenDB_6045(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void Overflows_overlapping_bug()
        {
            using (var tx = Env.WriteTransaction())
            {
                int numberOfPages;
                var page1 = tx.LowLevelTransaction.AllocateOverflowRawPage(Constants.Storage.PageSize - 4, out numberOfPages, zeroPage: true);

                Assert.Equal(2, numberOfPages);

                var page2 = tx.LowLevelTransaction.AllocateOverflowRawPage(Constants.Storage.PageSize - 96, out numberOfPages, zeroPage: true);

                Assert.Equal(1, numberOfPages);

                page2.PageNumber = 128;

                for (int i = 0; i < 4092; i++)
                {
                    page1.DataPointer[i] = 255;
                }

                Assert.Equal(128, page2.PageNumber);
                Assert.Equal(Constants.Storage.PageSize - 96, page2.OverflowSize);
                Assert.Equal(PageFlags.Overflow, page2.Flags);

                for (int i = 0; i < 4000; i++)
                {
                    Assert.Equal(0, page2.DataPointer[i]);
                }
            }
        }
    }
}
