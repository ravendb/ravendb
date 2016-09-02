using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Xunit;

namespace SlowTests.MailingList
{
    public class IdComesBackLowerCase : RavenTestBase
    {
        private readonly IDocumentStore _store;

        protected override void ModifyStore(DocumentStore store)
        {
            store.RegisterListener(new NoStaleQueriesListener());
        }

        public IdComesBackLowerCase()
        {
            _store = GetDocumentStore();

            new Product_AvailableForSale3().Execute(_store);

            var product1 = new Product("MyName1", "MyBrand1");
            product1.Id = "Products/100";

            var product2 = new Product("MyName2", "MyBrand2");
            product2.Id = "Products/101";

            var facetSetup = new FacetSetup { Id = "facets/ProductFacets", Facets = new List<Facet> { new Facet { Name = "Brand" } } };

            using (var docSession = _store.OpenSession())
            {
                foreach (var productDoc in docSession.Query<Product>())
                {
                    docSession.Delete(productDoc);
                }
                docSession.SaveChanges();

                docSession.Store(product1);
                docSession.Store(product2);
                docSession.Store(facetSetup);
                docSession.SaveChanges();
            }

            using (var session = _store.OpenSession())
            {
                var check = session.Query<Product>().ToList();
                Assert.Equal(check.Count, 2);
            }
        }

        [Fact]
        public void ShouldReturnMatchingProductWithGivenIdWhenSelectingAllFields()
        {
            using (var session = _store.OpenSession())
            {
                var products = session.Advanced.DocumentQuery<Product, Product_AvailableForSale3>()
                    .SelectFields<Product>()
                    .UsingDefaultField("Any")
                    .Where("MyName1").ToList();

                Assert.Equal("Products/100", products.First().Id);
            }
        }

        [Fact]
        public void ShouldReturnMatchingProductWithGivenId()
        {
            using (var session = _store.OpenSession())
            {
                var products = session.Advanced.DocumentQuery<Product, Product_AvailableForSale3>()
                    .UsingDefaultField("Any")
                    .Where("MyName1").ToList();

                Assert.Equal("Products/100", products.First().Id);
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

        private class NoStaleQueriesListener : IDocumentQueryListener
        {
            public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
            {
                queryCustomization.WaitForNonStaleResults(TimeSpan.FromSeconds(30));
            }
        }
    }
}
