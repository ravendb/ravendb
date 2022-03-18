using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class FacetsHits : RavenTestBase
    {
        public FacetsHits(ITestOutputHelper output) : base(output)
        {
        }

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
                                FieldName = "Category"
                            },
                        },
                        RangeFacets = new List<RangeFacet>()
                        {
                            new RangeFacet()
                            {
                                Ranges = new List<string>
                                {
                                    "Price <= 0",
                                    "Price BETWEEN 0.001 AND 0.999",
                                    "Price BETWEEN 0.999 AND 1.999",
                                    "Price >= 1.999"
                                }
                            }
                        }
                    };
                    session.Store(facetSetup);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var query = s.Query<Product>("Products/Stats");
                    var facetResults = query.AggregateUsing("facets/StatsFacet").Execute();

                    var priceFacet = facetResults["Price"];

                    foreach (var val in priceFacet.Values)
                        Assert.NotEqual(0, val.Count);
                }
            }
        }
    }
}
