using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
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
                FacetedQueryResult result = ExecuteTest(store);

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
                FacetedQueryResult result = ExecuteTest(store);

                //Assert
                CheckResults(result);
            }
        }

        private static void CheckResults(FacetedQueryResult result)
        {
            Assert.Contains("Brand", result.Results.Select(x => x.Key));
            FacetResult facetResult = result.Results["Brand"];
            Assert.Equal(1, facetResult.Values.Count);
            facetResult.Values[0] = new FacetValue { Range = "mybrand1", Hits = 1 };
        }

        private static FacetedQueryResult ExecuteTest(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                return session.Advanced.DocumentStore.Operations.Send(new GetMultiFacetsOperation(new FacetQuery()
                {
                    Query = "FROM INDEX 'Product/AvailableForSale2' WHERE Any = 'MyName1'",
                    DefaultField = "Any",
                    FacetSetupDoc = "facets/ProductFacets"
                }))[0];
            }
        }

        private static void SetupTestData(IDocumentStore store)
        {
            new Product_AvailableForSale2().Execute(store);

            Product product1 = new Product("MyName1", "MyBrand1");
            Product product2 = new Product("MyName2", "MyBrand2");

            FacetSetup facetSetup = new FacetSetup { Id = "facets/ProductFacets", Facets = new List<Facet> { new Facet { Name = "Brand" } } };

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
