// -----------------------------------------------------------------------
//  <copyright file="WithMapReduce.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.NestedIndexing
{
    public class WithMapReduce : RavenTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] ProductIds { get; set; }
        }
        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ZipCode { get; set; }
        }

        private class ProductSalesByZip : AbstractIndexCreationTask<Order, ProductSalesByZip.Result>
        {
            public class Result
            {
                public string Zip { get; set; }
                public string ProductId { get; set; }
                public int Count { get; set; }
            }

            public ProductSalesByZip()
            {
                Map = orders =>
                      from order in orders
                      let zip = LoadDocument<Customer>(order.CustomerId).ZipCode
                      from p in order.ProductIds
                      select new
                      {
                          Zip = zip,
                          ProductId = p,
                          Count = 1
                      };
                Reduce = results =>
                         from result in results
                         group result by new { result.Zip, result.ProductId }
                             into g
                         select new
                         {
                             g.Key.Zip,
                             g.Key.ProductId,
                             Count = g.Sum(x => x.Count)
                         };
            }
        }

        [Fact]
        public void CanUseReferencesFromMapReduceMap()
        {
            using (var store = GetDocumentStore())
            {
                new ProductSalesByZip().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product { Name = "Milk", Id = "products/milk" });
                    session.Store(new Product { Name = "Bear", Id = "products/bear" });

                    session.Store(new Customer { ZipCode = "1234", Name = "Ayende", Id = "customers/ayende" });
                    session.Store(new Customer { ZipCode = "4321", Name = "Rahien", Id = "customers/rahien" });

                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/milk" } });
                    session.Store(new Order { CustomerId = "customers/ayende", ProductIds = new[] { "products/bear", "products/milk" } });

                    session.Store(new Order { CustomerId = "customers/rahien", ProductIds = new[] { "products/bear" } });
                    session.Store(new Order { CustomerId = "customers/rahien", ProductIds = new[] { "products/bear", "products/milk" } });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<ProductSalesByZip.Result, ProductSalesByZip>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.Zip).ThenBy(x => x.ProductId)
                        .ToList();

                    Assert.Equal(4, results.Count);

                    Assert.Equal("1234", results[0].Zip);
                    Assert.Equal("products/bear", results[0].ProductId);
                    Assert.Equal(1, results[0].Count);

                    Assert.Equal("1234", results[1].Zip);
                    Assert.Equal("products/milk", results[1].ProductId);
                    Assert.Equal(3, results[1].Count);

                    Assert.Equal("4321", results[2].Zip);
                    Assert.Equal("products/bear", results[2].ProductId);
                    Assert.Equal(2, results[2].Count);

                    Assert.Equal("4321", results[3].Zip);
                    Assert.Equal("products/milk", results[3].ProductId);
                    Assert.Equal(1, results[3].Count);
                }
            }
        }
    }
}
