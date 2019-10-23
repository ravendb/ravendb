using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
   public class RavenDB_9679 : RavenTestBase
    {
        public RavenDB_9679(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Multi_map_index_using_the_same_collection()
        {
            using (var store = GetDocumentStore())
            {
                new MultiMapIndexTheSameCollection().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A",
                        Company = "companies/1-A",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 1,
                                PricePerUnit = 10
                            }
                        },
                        ShipTo = new Address
                        {
                            Country = "USA"
                        }
                    });

                    session.Store(new Order
                    {
                        Employee = "employees/2-A",
                        Company = "companies/2-A",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 1,
                                PricePerUnit = 10
                            }
                        },
                        ShipTo = new Address
                        {
                            Country = "Canada"
                        }
                    });

                    session.SaveChanges();

                    var orders = session.Query<Order, MultiMapIndexTheSameCollection>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(2, orders.Count);
                }
                
            }
        }

        public class MultiMapIndexTheSameCollection : AbstractMultiMapIndexCreationTask<Order>
        {
            public MultiMapIndexTheSameCollection()
            {
                AddMap<Order>(orders => from order in orders
                    where order.ShipTo.Country != "USA"
                    select new
                    {
                        order.Employee,
                        order.Company,
                        Total = 1.0m * order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                    });

                AddMap<Order>(orders => from order in orders
                    where order.ShipTo.Country == "USA"
                    select new
                    {
                        order.Employee,
                        order.Company,
                        Total = 1.15m * order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                    });
            }
        }
    }
}
