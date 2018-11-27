using Sparrow;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public unsafe class PageHandling : FastTests.Voron.StorageTest
    {
        [Fact]
        public void AllocateOverflowPages()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                int _;
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(16000, out _);
                page.DataPointer[15999] = 255;

                pageNumber = page.PageNumber;

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var readPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, readPage.DataPointer[15999]);

                var writePage = tx.LowLevelTransaction.ModifyPage(pageNumber);
                Assert.Equal(255, writePage.DataPointer[15999]);

                var anotherReadPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, anotherReadPage.DataPointer[15999]);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var readPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, readPage.DataPointer[15999]);
            }
        }

        [Fact]
        public void ReadModifyReadPagesInSameTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                int _;
                var allocatedPage = tx.LowLevelTransaction.AllocateOverflowRawPage(16000, out _);
                Memory.Set(allocatedPage.DataPointer, 0xFF, (long)16000);

                long pageNumber = allocatedPage.PageNumber;

                var readPage = tx.LowLevelTransaction.GetPage(pageNumber);
                for (int i = 0; i < 16000; i++)
                    Assert.Equal(0xFF, readPage.DataPointer[i]);

                var writePage = tx.LowLevelTransaction.ModifyPage(pageNumber);
                writePage.DataPointer[15999] = 0x22;

                var secondReadPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(0x22, secondReadPage.DataPointer[15999]);
                for (int i = 0; i < 15999; i++)
                    Assert.Equal(0xFF, secondReadPage.DataPointer[i]);

                tx.Commit();
            }
        }

        [Fact]
        public void EnsureDataPointerChangesAfterModifyInSameTransaction()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                int _;
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(16000, out _);
                pageNumber = page.PageNumber;

                var r1 = tx.LowLevelTransaction.GetPage(pageNumber);
                var r2 = tx.LowLevelTransaction.GetPage(pageNumber);
                var w1 = tx.LowLevelTransaction.ModifyPage(pageNumber);
                var r3 = tx.LowLevelTransaction.GetPage(pageNumber);

                // Allocation will ensure page is writable, then all pointers should be the same.
                Assert.Equal((long)r1.DataPointer, (long)r2.DataPointer);
                Assert.Equal((long)w1.DataPointer, (long)r3.DataPointer);
                Assert.Equal((long)r1.DataPointer, (long)w1.DataPointer);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var llt = tx.LowLevelTransaction;

                var r1 = tx.LowLevelTransaction.GetPage(pageNumber);
                var r2 = tx.LowLevelTransaction.GetPage(pageNumber);
                var w1 = tx.LowLevelTransaction.ModifyPage(pageNumber);
                var r3 = tx.LowLevelTransaction.GetPage(pageNumber);

                // We are not writable first, so after ModifyPage the data pointer must change. 
                Assert.Equal((long)r1.DataPointer, (long)r2.DataPointer);
                Assert.Equal((long)w1.DataPointer, (long)r3.DataPointer);
                Assert.NotEqual((long)r1.DataPointer, (long)w1.DataPointer);

                tx.Commit();
            }
        }
    }
}
