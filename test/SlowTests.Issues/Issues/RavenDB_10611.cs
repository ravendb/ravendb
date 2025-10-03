using System.Linq;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_10611 : RavenTestBase
    {
        public RavenDB_10611(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void DistinctCountShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        ShipTo = new Address
                        {
                            Country = "Poland"
                        }
                    });

                    session.Store(new Order
                    {
                        ShipTo = new Address
                        {
                            Country = "USA"
                        }
                    });

                    session.Store(new Order
                    {
                        ShipTo = new Address
                        {
                            Country = "Poland"
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var countries = session
                        .Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.ShipTo.Country)
                        .Select(x => x.ShipTo.Country)
                        .Distinct()
                        .ToList();

                    var count = session
                        .Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.ShipTo.Country)
                        .Select(x => x.ShipTo.Country)
                        .Distinct()
                        .Count();

                    Assert.Equal(2, countries.Count);
                    Assert.Equal(countries.Count, count);
                }
            }
        }
    }
}
