using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9936 : RavenTestBase
    {
        private class MixedSyntaxIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "MixedSyntaxIndex",
                    Maps =
                    {
                        "from o in docs.Orders.SelectMany(x => x.Lines) select new { ProductName = o.ProductName }"
                    }
                };
            }
        }

        [Fact]
        public void CanMixSyntaxInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new MixedSyntaxIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "HR",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "ABC"
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        Company = "CF",
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "CBA"
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var orders = session
                        .Advanced
                        .DocumentQuery<Order, MixedSyntaxIndex>()
                        .WhereEquals("ProductName", "ABC")
                        .ToList();

                    Assert.Equal(1, orders.Count);
                    Assert.Equal("HR", orders[0].Company);

                    orders = session
                        .Advanced
                        .DocumentQuery<Order, MixedSyntaxIndex>()
                        .WhereEquals("ProductName", "A")
                        .ToList();

                    Assert.Equal(0, orders.Count);
                }
            }
        }
    }
}
