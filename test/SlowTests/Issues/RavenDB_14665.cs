using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14665 : RavenTestBase
    {
        public RavenDB_14665(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IndexEtagForReferencedDocumentsNeedsToBeCalculatedCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                new Products_BySupplier().Execute(store);

                using (var session = store.OpenSession())
                {
                    var supplier1 = new Supplier { Id = "suppliers/1", Name = "CF" };
                    var supplier2 = new Supplier { Id = "suppliers/2", Name = "IN" };
                    var supplier3 = new Supplier { Id = "suppliers/3", Name = "YR" };
                    var supplier4 = new Supplier { Id = "suppliers/4", Name = "XX" };

                    session.Store(supplier1);
                    session.Store(supplier2);
                    session.Store(supplier3);
                    session.Store(supplier4);

                    var product1 = new Product { Name = "HR", Supplier = supplier1.Id };
                    var product2 = new Product { Name = "HR", Supplier = supplier2.Id };
                    var product3 = new Product { Name = "UR", Supplier = supplier3.Id };
                    var product4 = new Product { Name = "II", Supplier = supplier4.Id };

                    session.Store(product1);
                    session.Store(product2);
                    session.Store(product3);
                    session.Store(product4);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_BySupplier>()
                        .Statistics(out var statistics)
                        .ToList();

                    Assert.Equal(4, products.Count);
                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0);
                }

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_BySupplier>()
                        .Statistics(out var statistics)
                        .ToList();

                    Assert.Equal(4, products.Count);
                    Assert.False(statistics.IsStale);
                    Assert.Equal(-1, statistics.DurationInMs); // from cache
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("suppliers/4");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Query<Product, Products_BySupplier>()
                        .Statistics(out var statistics)
                        .ToList();

                    Assert.Equal(4, products.Count);
                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // detected that staleness changed
                    var resultEtag1 = statistics.ResultEtag;

                    session.Delete("suppliers/1");
                    session.SaveChanges();

                    store.Maintenance.Send(new StartIndexingOperation());

                    Indexes.WaitForIndexing(store);

                    store.Maintenance.Send(new StopIndexingOperation());

                    session.Delete("suppliers/2");
                    session.SaveChanges();

                    products = session.Query<Product, Products_BySupplier>()
                        .Statistics(out statistics)
                        .ToList();

                    Assert.Equal(2, products.Count);
                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // we should not use cache
                    var resultEtag2 = statistics.ResultEtag;

                    Assert.NotEqual(resultEtag1, resultEtag2);
                }
            }
        }

        private class Products_BySupplier : AbstractIndexCreationTask<Product>
        {
            public Products_BySupplier()
            {
                Map = products => from p in products
                                  let supplier = LoadDocument<Supplier>(p.Supplier)
                                  select new
                                  {
                                      Name = supplier.Name
                                  };
            }
        }
    }
}
