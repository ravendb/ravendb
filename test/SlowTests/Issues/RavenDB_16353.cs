using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16353 : RavenTestBase
    {
        public RavenDB_16353(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Use_No_Tracking_For_Referenced_Items()
        {
            var productsBySupplierNoTracking = new Products_BySupplier_NoTracking();
            var productsBySupplier = new Products_BySupplier();

            var map = productsBySupplierNoTracking.CreateIndexDefinition().Maps.First();
            RavenTestHelper.AssertEqualRespectingNewLines(
                "docs.Products.Select(product => new {\r\n    product = product,\r\n    supplier = this.NoTracking.LoadDocument(product.Supplier, \"Suppliers\")\r\n}).Select(this0 => new {\r\n    Name = this0.supplier.Name\r\n})",
                map);

            await Can_Use_No_Tracking_For_Referenced_Items_Internal(productsBySupplierNoTracking, productsBySupplier);
        }

        [Fact]
        public async Task Can_Use_No_Tracking_For_Referenced_Items_JavaScript()
        {
            var productsBySupplierNoTracking = new Products_BySupplier_NoTracking_JavaScript();
            var productsBySupplier = new Products_BySupplier_JavaScript();

            await Can_Use_No_Tracking_For_Referenced_Items_Internal(productsBySupplierNoTracking, productsBySupplier);
        }

        private async Task Can_Use_No_Tracking_For_Referenced_Items_Internal(AbstractIndexCreationTask productsBySupplierNoTracking, AbstractIndexCreationTask productsBySupplier)
        {
            using (var store = GetDocumentStore())
            {
                await productsBySupplierNoTracking.ExecuteAsync(store);
                await productsBySupplier.ExecuteAsync(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var i = database.IndexStore.GetIndex(productsBySupplierNoTracking.IndexName);
                Assert.Empty(i.GetReferencedCollections());

                i = database.IndexStore.GetIndex(productsBySupplier.IndexName);
                Assert.NotEmpty(i.GetReferencedCollections());

                using (var session = store.OpenAsyncSession())
                {
                    var supplier = new Supplier { Name = "Bob" };

                    await session.StoreAsync(supplier, "suppliers/1");

                    var product = new Product { Name = "Cheese", Supplier = supplier.Id };

                    await session.StoreAsync(product, "products/1");

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var terms = await store.Maintenance.SendAsync(new GetTermsOperation(productsBySupplierNoTracking.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("bob", terms[0]);

                terms = await store.Maintenance.SendAsync(new GetTermsOperation(productsBySupplier.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("bob", terms[0]);

                using (var session = store.OpenAsyncSession())
                {
                    var supplier = await session.LoadAsync<Supplier>("suppliers/1");
                    supplier.Name = "John";

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                terms = await store.Maintenance.SendAsync(new GetTermsOperation(productsBySupplierNoTracking.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("bob", terms[0]);

                terms = await store.Maintenance.SendAsync(new GetTermsOperation(productsBySupplier.IndexName, "Name", fromValue: null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("john", terms[0]);
            }
        }

        private class Products_BySupplier_NoTracking : AbstractIndexCreationTask<Product>
        {
            public Products_BySupplier_NoTracking()
            {
                Map = products => from product in products
                                  let supplier = NoTracking.LoadDocument<Supplier>(product.Supplier)
                                  select new
                                  {
                                      Name = supplier.Name
                                  };
            }
        }

        private class Products_BySupplier : AbstractIndexCreationTask<Product>
        {
            public Products_BySupplier()
            {
                Map = products => from product in products
                                  let supplier = LoadDocument<Supplier>(product.Supplier)
                                  select new
                                  {
                                      Name = supplier.Name
                                  };
            }
        }

        private class Products_BySupplier_NoTracking_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Products_BySupplier_NoTracking_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    @"map('Products', function (p) { return { Name: noTracking.load(p.Supplier, 'Suppliers').Name };})"
                };
            }
        }

        private class Products_BySupplier_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Products_BySupplier_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    @"map('Products', function (p) { return { Name: load(p.Supplier, 'Suppliers').Name };})"
                };
            }
        }
    }
}
