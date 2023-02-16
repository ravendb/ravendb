using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18784 : RavenTestBase
{
    public RavenDB_18784(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void DynamicMapReduceQueryProjectionsOnShardedDatabase()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order {ShipTo = new Address {City = "Torun"}, Employee = "employees/1"});
                session.Store(new Order {ShipTo = new Address {City = "Gdansk"}, Employee = "employees/1"});
                session.Store(new Order {ShipTo = new Address {City = "Torun"}, Employee = "employees/2"});
                session.Store(new Order {ShipTo = new Address {City = "Warszawa"}, Employee = "employees/2"});
                session.Store(new Order {ShipTo = new Address {City = "Gdansk"}, Employee = "employees/1"});

                session.Store(new Employee { FirstName = "Jan"}, "employees/1");
                session.Store(new Employee { FirstName = "Adam"}, "employees/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var orders = session.Advanced.RawQuery<dynamic>("from Orders group by ShipTo.City select count(), ShipTo.City").WaitForNonStaleResults().ToList();

                Assert.Equal(3, orders.Count);

                foreach (dynamic order in orders)
                {
                    Assert.NotNull(order["Count"].Value);
                    Assert.NotNull(order["ShipTo.City"].Value);
                }

                var orders2 = session.Advanced.RawQuery<dynamic>("from Orders group by ShipTo.City select count(), ShipTo.City as c").WaitForNonStaleResults().ToList();

                Assert.Equal(3, orders2.Count);

                foreach (dynamic order in orders2)
                {
                    Assert.NotNull(order["Count"].Value);
                    Assert.NotNull(order["c"].Value);
                }

                var orders3 = session.Advanced.RawQuery<dynamic>("from Orders group by ShipTo.City select count() as MyCount, ShipTo.City as c").WaitForNonStaleResults().ToList();

                Assert.Equal(3, orders3.Count);

                foreach (dynamic order in orders3)
                {
                    Assert.NotNull(order["MyCount"].Value);
                    Assert.NotNull(order["c"].Value);
                }

                var ordersWithEmployeesIncluded = session.Advanced.RawQuery<dynamic>("from Orders group by Employee select Employee, count() include Employee").WaitForNonStaleResults().ToList();

                Assert.Equal(2, ordersWithEmployeesIncluded.Count);

                int requestsCount = session.Advanced.NumberOfRequests;

                foreach (dynamic order in ordersWithEmployeesIncluded)
                {
                    Assert.NotNull(order["Count"].Value);
                    Assert.NotNull(order["Employee"].Value);
                    Assert.NotNull(session.Load<Employee>(order["Employee"].Value));
                }

                Assert.Equal(requestsCount, session.Advanced.NumberOfRequests);
            }
        }
    }
}
