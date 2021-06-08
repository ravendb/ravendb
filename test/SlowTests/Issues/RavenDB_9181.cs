using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9181 : RavenTestBase
    {
        public RavenDB_9181(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_project_object_to_a_single_field()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        ShipTo = new Address
                        {
                            City = "Palo Alto",
                            Country = "USA"
                        }

                    }, "orders/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var q1 = (from o in session.Query<Order>()
                              select new
                              {
                                  Address = o.ShipTo
                              }).ToList();

                    Assert.NotNull(q1[0].Address);

                    var q2 = session.Query<Order>()
                                    .Select(o => o.ShipTo)
                                    .ToList();
                    Assert.IsType<Address>(q2[0]);

                }
            }
        }

    }
}
