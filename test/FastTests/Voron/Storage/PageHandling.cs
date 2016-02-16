using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Voron.Storage
{
    public unsafe class PageHandling : StorageTest
    {
        [Fact]
        public void AllocateOverflowPages()
        {
            long pageNumber;
            using (var tx = Env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.AllocateOverflowPage(16000);
                page.DataPointer[15000] = 255;

                pageNumber = page.PageNumber;

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var readPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, readPage.DataPointer[15000]);

                var writePage = tx.LowLevelTransaction.ModifyPage(pageNumber);
                Assert.Equal(255, writePage.DataPointer[15000]);

                var anotherReadPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, anotherReadPage.DataPointer[15000]);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var readPage = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(255, readPage.DataPointer[15000]);
            }
        }
    }
}
