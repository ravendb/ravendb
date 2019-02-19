using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class FacetQueryNotCorrectWithDefaultFieldSpecified : RavenTestBase
    {
        /// <summary>
        /// Works
        /// </summary>
        [Fact]
        public void ShouldWorkWithEmbeddedRaven()
        {
            //arrange
            using (var store = GetDocumentStore())
            {
                SetupTestData(store);

                WaitForIndexing(store);

                //Act
                var result = ExecuteTest(store);

                //Assert
                CheckResults(result);
            }
        }

        /// <summary>
        /// Should work but does not
        /// </summary>
        [Fact]
        public void ShouldWorkWithRavenServer()
        {
            //arrange
            using (var store = GetDocumentStore())
            {
                SetupTestData(store);

                WaitForIndexing(store);

                //Act
                var result = ExecuteTest(store);

                //Assert
                CheckResults(result);
            }
        }

        private static void CheckResults(Dictionary<string, FacetResult> result)
        {
            Assert.Contains("Brand", result.Select(x => x.Key));
            FacetResult facetResult = result["Brand"];
            Assert.Equal(1, facetResult.Values.Count);
            facetResult.Values[0] = new FacetValue { Range = "mybrand1", Count = 1 };
        }

        private static Dictionary<string, FacetResult> ExecuteTest(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                return session.Advanced.DocumentQuery<Product, Product_AvailableForSale2>()
                    .WhereEquals("Any", "MyName1")
                    .AggregateUsing("facets/ProductFacets")
                    .Execute();
            }
        }

        private static void SetupTestData(IDocumentStore store)
        {
            new Product_AvailableForSale2().Execute(store);

            Product product1 = new Product("MyName1", "MyBrand1");
            Product product2 = new Product("MyName2", "MyBrand2");

            FacetSetup facetSetup = new FacetSetup { Id = "facets/ProductFacets", Facets = new List<Facet> { new Facet { FieldName = "Brand" } } };

            using (IDocumentSession docSession = store.OpenSession())
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

        private class Product_AvailableForSale2 : AbstractIndexCreationTask<Product>
        {
            public Product_AvailableForSale2()
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
