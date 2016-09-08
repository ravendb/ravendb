using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class FacetsMultipleAggregation : RavenTestBase
    {
        [Fact]
        public void CanAggregateByMinAndMaxOnSameField()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new FacetSetup { Facets = GetFacets(), Id = "facets/Products" });
                    session.Store(GetProduct("1", "Blue Jeans", 100));
                    session.Store(GetProduct("2", "Green Top", 150));
                    session.Store(GetProduct("3", "Red Shoes", 200));
                    session.SaveChanges();

                    new Products().Execute(store);

                    WaitForIndexing(store);

                    var query = session.Advanced.DocumentQuery<Product>(new Products().IndexName);
                    var results = query.ToFacets("facets/Products");
                    Assert.Equal(100, results.Results["Prices"].Values.First().Min);
                    Assert.Equal(200, results.Results["PricesMax"].Values.First().Max);
                }
            }
        }

        private List<Facet> GetFacets()
        {
            return new List<Facet>
            {
                new Facet
                {
                    Name = "Prices",
                    DisplayName = "Prices",
                    Mode = FacetMode.Ranges,
                    Aggregation = FacetAggregation.Min,
                    AggregationField = "Prices_Range",
                    TermSortMode = FacetTermSortMode.ValueAsc,
                    Ranges = {"[* TO *]"}
                },
                new Facet
                {
                    Name = "Prices",
                    DisplayName = "PricesMax",
                    Mode = FacetMode.Ranges,
                    MaxResults = 1,
                    Aggregation = FacetAggregation.Max,
                    AggregationField = "Prices_Range",
                    Ranges = {"[* TO *]"}
                },
            };
        }

        private Product GetProduct(string id, string name, decimal price)
        {
            return new Product
            {
                Id = id,
                Name = name,
                Variants = new List<Variant>
                {
                    new Variant
                    {
                        ListPrice = new Price
                        {
                            Amount = price,
                            Currency = "GBP"
                        }
                    }
                }
            };
        }

        private class Variant
        {
            public Price ListPrice { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IEnumerable<Variant> Variants { get; set; }
        }

        private class Price
        {
            public decimal Amount { get; set; }
            public string Currency { get; set; }
        }

        private class ProductQuery
        {
            public string Name { get; set; }
            public IEnumerable<decimal> Prices { get; set; }
        }

        private class Products : AbstractIndexCreationTask<Product, ProductQuery>
        {
            public Products()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        p.Name,
                        Prices = p.Variants.Select(v => (decimal)v.ListPrice.Amount).Distinct(),
                    };

                Sort(e => e.Prices, SortOptions.NumericDouble);
            }
        }
    }
}