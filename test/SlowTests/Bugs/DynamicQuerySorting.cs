using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DynamicQuerySorting : RavenTestBase
    {
        public DynamicQuerySorting(ITestOutputHelper output) : base(output)
        {
        }

        private class GameServer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldSelectIndexWhenNoSortingSpecified(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<GameServer>()
                        .Statistics(out stats)
                        .OrderBy(x => x.Name)
                        .ToList();
                }

                var indexQuery = new IndexQuery { Query = "FROM GameServers ORDER BY Name ASC" };

                var indexName = store.Commands().Query(indexQuery).IndexName;
                Assert.Equal(stats.IndexName, indexName);
            }
        }
    }
}
