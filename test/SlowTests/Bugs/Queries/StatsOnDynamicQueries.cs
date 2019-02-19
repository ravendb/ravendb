using FastTests;
using System.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Bugs.Queries
{
    public class StatsOnDynamicQueries : RavenTestBase
    {
        [Fact]
        public void WillGiveStats()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 15,
                        Email = "ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Email == "ayende")
                        .ToArray();

                    Assert.NotEqual(0, stats.TotalResults);
                }
            }
        }

        [Fact]
        public void WillGiveStatsForLuceneQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 15,
                        Email = "ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Advanced.DocumentQuery<User>()
                        .Statistics(out stats)
                        .WhereLucene("Email", "ayende")
                        .WaitForNonStaleResults();

                    var result = query.ToArray();
                    Assert.NotEqual(0, stats.TotalResults);
                    Assert.Equal(stats.TotalResults, query.GetQueryResult().TotalResults);
                    Assert.Equal("Auto/Users/BySearch(Email)", stats.IndexName);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
