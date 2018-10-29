using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11443 : RavenTestBase
    {
        private class Index : AbstractIndexCreationTask<Product>
        {
            public Index()
            {
                Map = products => from product in products
                                  let category = LoadDocument<Category>(product.Category)
                                  select new
                                  {
                                      CategoryId = category.Id
                                  };
            }
        }

        [Fact]
        public void CanTranslateProperlyIdToMethodWhenLoadDocumentIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                var indexDefinition = index.CreateIndexDefinition();

                Assert.Contains("CategoryId = Id(this0.category)", indexDefinition.Maps.First());

                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Id = "products/1",
                        Name = "P1",
                        Category = "categories/1"
                    });

                    session.Store(new Category
                    {
                        Id = "categories/1",
                        Name = "C1",
                        Description = "D1"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var products = session
                        .Advanced
                        .DocumentQuery<Product, Index>()
                        .WhereEquals("CategoryId", "categories/1")
                        .ToList();

                    Assert.Equal(1, products.Count);
                }
            }
        }
    }
}
