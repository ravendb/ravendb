using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Bugs
{
    public class DynamicQuerySorting : RavenTestBase
    {
        private class GameServer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void ShouldSelectIndexWhenNoSortingSpecified()
        {
            using (var store = GetDocumentStore())
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
