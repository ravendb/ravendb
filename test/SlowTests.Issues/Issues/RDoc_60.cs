using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDoc_60 : RavenTestBase
    {
        public RDoc_60(ITestOutputHelper output) : base(output)
        {
        }

        private class Order
        {
            public string Id { get; set; }

            public IList<LineItem> LineItems { get; set; }
        }

        private class LineItem
        {
            public string Id { get; set; }

            public string ProductId { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void IncludeShouldWorkForStringIdentifiers()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var order = new Order
                    {
                        Id = "orders/1",
                        LineItems = new List<LineItem>
                                                {
                                                    new LineItem
                                                    {
                                                        Id = "lineItems/1",
                                                        ProductId = "products/1"
                                                    }
                                                }
                    };

                    var product = new Product
                    {
                        Id = "products/1",
                        Name = "Product 1"
                    };

                    session.Store(product);
                    session.Store(order);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session
                        .Include<Order, Product>(x => x.LineItems.Select(li => li.ProductId))
                        .Load("orders/1");

                    foreach (var lineItem in order.LineItems)
                    {
                        var product = session.Load<Product>(lineItem.ProductId);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}