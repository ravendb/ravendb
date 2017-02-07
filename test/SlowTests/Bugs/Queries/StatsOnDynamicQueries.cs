using FastTests;
using Raven.NewClient.Client;
using System.Linq;
using Xunit;

namespace SlowTests.Bugs.Queries
{
    public class StatsOnDynamicQueries : RavenNewTestBase
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
                    RavenQueryStatistics stats;
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x=>x.Email == "ayende")
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
                    RavenQueryStatistics stats;
                    var query = session.Advanced.DocumentQuery<User>()
                        .Statistics(out stats)
                        .Where("Email:ayende");

                    var result = query.ToArray();
                    Assert.NotEqual(0, stats.TotalResults);
                    Assert.Equal(stats.TotalResults, query.QueryResult.TotalResults);
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
