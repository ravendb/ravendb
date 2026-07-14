using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class RavenDB_27029(ITestOutputHelper output) : TableStorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void GetPageOwners_MapsLargeValueOverflowPages()
        {
            const string key = "users/1";

            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                // A value comfortably larger than RawDataSection.MaxItemSize forces the row to be
                // stored as standalone overflow pages instead of inside a small RawData section.
                var big = new string('x', 32 * 1024);
                SetHelper(docs, key, "Users", 1L, big);
                tx.Commit();
            }

            long pageNumber;
            int overflowPageCount;
            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice.From(tx.Allocator, key, out var k);
                Assert.True(docs.ReadByKey(k, out var reader));

                // Large values live on dedicated overflow pages, so their id is page-aligned.
                Assert.Equal(0, reader.Id % Constants.Storage.PageSize);
                pageNumber = reader.Id / Constants.Storage.PageSize;

                var page = tx.LowLevelTransaction.GetPage(pageNumber);
                Assert.True(page.IsOverflow);
                overflowPageCount = Paging.GetNumberOfOverflowPages(page.OverflowSize);
            }

            using (var tx = Env.WriteTransaction())
            {
                var owners = Env.GetPageOwners(tx);

                // Every page of the large value (header + continuations) must be attributed to the
                // table, otherwise the storage-pages debug endpoint reports them as false gaps.
                for (long p = pageNumber; p < pageNumber + overflowPageCount; p++)
                {
                    Assert.Contains(p, owners.Keys);
                    Assert.EndsWith("/LargeValue", owners[p]);
                }
            }
        }
    }
}
