using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.MailingList
{
    public class IdComesBackLowerCase : RavenTestBase
    {
        private static void CreateData(IDocumentStore store)
        {
            new Product_AvailableForSale3().Execute(store);

            var product1 = new Product("MyName1", "MyBrand1");
            product1.Id = "Products/100";

            var product2 = new Product("MyName2", "MyBrand2");
            product2.Id = "Products/101";

            var facetSetup = new FacetSetup { Id = "facets/ProductFacets", Facets = new List<Facet> { new Facet { FieldName = "Brand" } } };

            using (var docSession = store.OpenSession())
            {
                foreach (var productDoc in docSession.Query<Product>().Customize(x => x.WaitForNonStaleResults()))
                {
                    docSession.Delete(productDoc);
                }
                docSession.SaveChanges();

                docSession.Store(product1);
                docSession.Store(product2);
                docSession.Store(facetSetup);
                docSession.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var check = session.Query<Product>().ToList();
                Assert.Equal(check.Count, 2);
            }
        }

        [Fact]
        public void ShouldReturnMatchingProductWithGivenIdWhenSelectingAllFields()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_AvailableForSale3>()
                        .WaitForNonStaleResults()
                        .SelectFields<Product>()
                        .WhereLucene("Any", "MyName1")
                        .ToList();

                    Assert.Equal("Products/100", products.First().Id);
                }
            }
        }

        [Fact]
        public void ShouldReturnMatchingProductWithGivenId()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_AvailableForSale3>()
                        .WaitForNonStaleResults()
                        .WhereLucene("Any", "MyName1")
                        .ToList();

                    Assert.Equal("Products/100", products.First().Id);
                }
            }
        }

        private class Product
        {
            public Product(string name, string brand)
            {
                Name = name;
                Brand = brand;
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public string Brand { get; set; }
        }

        private class Product_AvailableForSale3 : AbstractIndexCreationTask<Product>
        {
            public Product_AvailableForSale3()
            {
                Map = products => from p in products
                                  select new
                                  {
                                      p.Name,
                                      p.Brand,
                                      Any = new object[]
                                              {
                                                  p.Name,
                                                  p.Brand
                                              }
                                  };
            }
        }
    }
}
