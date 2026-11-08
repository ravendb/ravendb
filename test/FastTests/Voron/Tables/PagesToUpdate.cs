using System;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class PagesToUpdate(ITestOutputHelper output) : TableStorageTest(output)
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PagesInScratchAreNotReadAhead()
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, "docs", 16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                SetHelper(tx.OpenTable(DocsSchema, "docs"), "users/1", "Users", 1L, "{'Name': 'Oren'}");
                tx.Commit();
            }

            Span<long> pages = stackalloc long[2];
            Assert.Equal(0, CountPagesToUpdate(pages)); // written, not yet flushed to the data file: the pages are in scratch

            Env.FlushLogToDataFile();
            Assert.Equal(2, CountPagesToUpdate(pages)); // the value's page and its section's header page
        }

        private int CountPagesToUpdate(Span<long> pages)
        {
            using (var tx = Env.ReadTransaction())
            {
                var docs = tx.OpenTable(DocsSchema, "docs");
                Slice.From(tx.Allocator, "users/1", out Slice key);
                Assert.True(docs.ReadByKey(key, out TableValueReader value));
                return docs.GetPagesToUpdate(value.Id, pages);
            }
        }
    }
}
