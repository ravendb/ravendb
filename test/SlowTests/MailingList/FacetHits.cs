using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.MailingList
{
    public class FacetsHits : RavenTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Category { get; set; }
            public Price Price { get; set; }
        }

        private class Price
        {
            public float Amount { get; set; }
            public string Currency { get; set; }
        }


        private class Products_Stats : AbstractIndexCreationTask<Product>
        {
            public Products_Stats()
            {
                Map = products =>
                      from product in products
                      select new
                      {
                          Category = product.Category,
                          Price = product.Price.Amount

                      };

            }
        }

        [Fact]
        public void CanSearchOnAllProperties()
        {
            using (var store = GetDocumentStore())
            {

                new Products_Stats().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        var amount = i % 50 / 10f;
                        session.Store(
                            new Product
                            {
                                Category = i % 2 == 0 ? "Cat1" : "Cat2",
                                Name = "Product " + i,
                                Price = new Price { Currency = "USD", Amount = amount }
                            });
                    }

                    var facetSetup = new FacetSetup
                    {
                        Id = "facets/StatsFacet",
                        Facets = new List<Facet>
                        {
                            new Facet
                            {
                                Name = "Category"
                            },
                            new Facet
                            {
                                Name = "Price_D_Range",
                                Ranges = new List<string>
                                {
                                    "[NULL TO 0]",
                                    "[0.001 TO 0.999]",
                                    "[0.999 TO 1.999]",
                                    "[1.999 TO NULL]"
                                }
                            }
                        }
                    };
                    session.Store(facetSetup);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var query = s.Query<Product>("Products/Stats");
                    var facetResults = query.AggregateUsing("facets/StatsFacet").ToDictionary();

                    var priceFacet = facetResults["Price_D_Range"];

                    foreach (var val in priceFacet.Values)
                        Assert.NotEqual(0, val.Hits);
                }
            }
        }
    }
}
