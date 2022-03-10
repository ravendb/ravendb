using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10640 : RavenTestBase
    {
        public RavenDB_10640(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task LazyQueryCachingTest(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new DocsIndex().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    QueryStatistics stats;

                    await session.Query<Doc, DocsIndex>().Statistics(out stats).ToListAsync();
                    Assert.NotEqual(-1, stats.DurationInMs);
                    await session.Query<Doc, DocsIndex>().Statistics(out stats).ToListAsync();
                    Assert.Equal(-1, stats.DurationInMs); // query cached

                    await session.Query<Doc, DocsIndex>().Statistics(out stats).Where(x => x.Id == "doc-1").LazilyAsync().Value;
                    Assert.NotEqual(-1, stats.DurationInMs);
                    await session.Query<Doc, DocsIndex>().Statistics(out stats).Where(x => x.Id == "doc-1").LazilyAsync().Value;
                    Assert.Equal(-1, stats.DurationInMs);
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
