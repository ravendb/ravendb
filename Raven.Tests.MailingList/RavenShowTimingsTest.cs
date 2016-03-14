using System.Linq;
using Raven.Client;
using Raven.Tests.Helpers;
using Xunit;

namespace RavenLoadTest_Test
{
    public class RavenShowTimingsTest : RavenTestBase
    {
        [Fact]
        public void QueryingACollectionShowsTimings()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee());
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics queryStats;
                    var results = session.Query<Employee>()
                        .Customize(x =>
                        {
                            x.ShowTimings();
                            x.NoCaching();
                        })
                        .Statistics(out queryStats)
                        .ToList();

                    Assert.NotEmpty(queryStats.TimingsInMilliseconds);
                }
            }
        }

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }
    }
}
