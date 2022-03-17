using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13870 : RavenTestBase
    {
        public RavenDB_13870(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ArtificialProjectionsShouldNotBeTreatedAsProjections()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_FanOut().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "Company1",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "Product1"
                            },
                            new OrderLine
                            {
                                ProductName = "Product1"
                            }
                        }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results1 = session
                        .Advanced
                        .RawQuery<Order>("from index 'Orders/FanOut' as doc")
                        .ToList();

                    Assert.Equal(1, results1.Count);

                    var results2 = session
                        .Advanced
                        .RawQuery<Order>("from index 'Orders/FanOut' as doc select doc")
                        .ToList();

                    Assert.Equal(1, results2.Count);

                    var results3 = session
                        .Advanced
                        .RawQuery<Order>("from index 'Orders/FanOut' as doc select distinct doc")
                        .ToList();

                    Assert.Equal(1, results3.Count);
                }
            }
        }

        private class Orders_FanOut : AbstractIndexCreationTask<Order>
        {
            public Orders_FanOut()
            {
                Map = orders => from o in orders
                                from l in o.Lines
                                select new
                                {
                                    o.Id,
                                    o.Company,
                                    l.ProductName
                                };
            }
        }
    }
}
