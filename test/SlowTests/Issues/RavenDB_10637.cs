using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10637 : RavenTestBase
    {
        public RavenDB_10637(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TestLazyQueryStatsTest()
        {
            using (var store = GetDocumentStore())
            {
                var docsIndex = new DocsIndex();
                await docsIndex.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc());
                    await session.SaveChangesAsync();
                }
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Doc, DocsIndex>();

                    var lazyCount = await query.Statistics(out var stats).CountLazilyAsync().Value;
                    Assert.Equal(1, lazyCount);
                    Assert.Equal(docsIndex.IndexName, stats.IndexName);
                    Assert.NotEqual(default(DateTime), stats.Timestamp);
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Doc, DocsIndex>();

                    var lazyCount = query.Statistics(out var stats).CountLazily().Value;
                    Assert.Equal(1, lazyCount);
                    Assert.Equal(docsIndex.IndexName, stats.IndexName);
                    Assert.NotEqual(default(DateTime), stats.Timestamp);
                }
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public int IntVal { get; set; }
        }

        private class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.IntVal,
                    };
            }
        }
    }
}
