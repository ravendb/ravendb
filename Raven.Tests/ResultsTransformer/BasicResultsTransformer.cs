using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client;
using Raven.Client.Linq;

namespace Raven.Tests.ResultsTransformer
{
    public class StronglyTypedResultsTransformer : RavenTest
    {
        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        public class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] ProductIds { get; set; }
        }
        public class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ZipCode { get; set; }
        }

        public class OrderWithProductInformation : AbstractResultsTransformer<Order>
        {
            public class Result
            {
                public string OrderId { get; set; }
                public string ProductId { get; set; }
                public string CustomerId { get; set; }
                public string ProductName { get; set; }
            }
            public OrderWithProductInformation()
            {
                TransformResults = (database, orders) => from doc in orders
                                                         from productid in doc.ProductIds
                                                         let product = database.Load<Product>(productid)
                                                         select new
                                                         {
                                                             OrderId = doc.Id,
                                                             ProductId = product.Id,
                                                             ProductName = product.Name
                                                         };
            }

        }


        [Fact]
        public void CanUseResultsTransformerOnDynamicQuery()
        {
            using (var store = NewDocumentStore())
            {
                new OrderWithProductInformation().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product{ Name = "Milk", Id = "products/milk" });
                    session.Store(new Product{ Name = "Bear", Id = "products/bear" });

                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/bear", "products/milk" } });

                    session.Store(new Order { CustomerId = "customers/rahien", ProductIds = new[] { "products/bear" } });
                    session.Store(new Order { CustomerId = "customers/bob", ProductIds = new[] { "products/bear", "products/milk" } });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(order => order.CustomerId == "customers/bob")
                        .TransformWith<OrderWithProductInformation, OrderWithProductInformation.Result>()
                        .ToList()
                        // client side!
                        .OrderBy(x => x.ProductName)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal("Milk", results[1].ProductName);
                    Assert.Equal("products/milk", results[1].ProductId);

                    Assert.Equal("Bear", results[0].ProductName);
                    Assert.Equal("products/bear", results[0].ProductId);
                }
            }
        }
    }

}
