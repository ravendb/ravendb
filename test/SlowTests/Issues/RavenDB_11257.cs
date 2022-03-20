using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11257 : RavenTestBase
    {
        public RavenDB_11257(ITestOutputHelper output) : base(output)
        {
        }

        private class Products_ByName : AbstractIndexCreationTask<Product>
        {
            public Products_ByName()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      Name = p.Name
                                  };
            }
        }

        [Fact]
        public void RandomOrderingShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Products_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 50; i++)
                        session.Store(new Product { Name = $"Product_{i:D5}" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var productNames = new HashSet<string>();

                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var product = session.Query<Product, Products_ByName>()
                            .Customize(x =>
                            {
                                x.RandomOrdering();
                                x.WaitForNonStaleResults();
                            })
                            .Statistics(out var stats)
                            .First(x => x.Name != Guid.Empty.ToString());

                        Assert.NotNull(product);
                        Assert.Equal(new Products_ByName().IndexName, stats.IndexName);

                        productNames.Add(product.Name);
                    }
                }

                Assert.True(productNames.Count > 1, $"Static: {productNames.Count} > 1");

                productNames = new HashSet<string>();

                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var product = session.Query<Product>()
                            .Customize(x =>
                            {
                                x.RandomOrdering();
                                x.WaitForNonStaleResults();
                            })
                            .Statistics(out var stats)
                            .First();

                        Assert.NotNull(product);
                        Assert.Equal("Auto/Products/ById()", stats.IndexName);

                        productNames.Add(product.Name);
                    }
                }

                Assert.True(productNames.Count > 1, $"Dynamic 1: {productNames.Count} > 1");

                productNames = new HashSet<string>();

                for (var i = 0; i < 50; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var product = session.Query<Product>()
                            .Customize(x =>
                            {
                                x.RandomOrdering();
                                x.WaitForNonStaleResults();
                            })
                            .Statistics(out var stats)
                            .First(x => x.Name != Guid.Empty.ToString());

                        Assert.NotNull(product);
                        Assert.Equal("Auto/Products/ByName", stats.IndexName);

                        productNames.Add(product.Name);
                    }
                }

                Assert.True(productNames.Count > 1, $"Dynamic 2: {productNames.Count} > 1");
            }
        }
    }
}
