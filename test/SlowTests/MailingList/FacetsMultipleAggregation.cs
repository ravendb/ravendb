using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class FacetsMultipleAggregation : RavenTestBase
    {
        public FacetsMultipleAggregation(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Facets)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanAggregateByMinAndMaxOnSameField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new FacetSetup { Facets = GetFacets(), Id = "facets/Products" });
                    session.Store(GetProduct("1", "Blue Jeans", 100));
                    session.Store(GetProduct("2", "Green Top", 150));
                    session.Store(GetProduct("3", "Red Shoes", 200));
                    session.SaveChanges();

                    new Products().Execute(store);

                    Indexes.WaitForIndexing(store);

                    var query = session.Advanced.DocumentQuery<Product>(new Products().IndexName);
                    var results = query.Take(1).AggregateUsing("facets/Products").Execute();
                    Assert.Equal(100, results["Prices"].Values.First().Min);
                    Assert.Equal(200, results["PricesMax"].Values.First().Max);
                }
            }
        }

        private List<Facet> GetFacets()
        {
            return new List<Facet>
            {
                new Facet
                {
                    FieldName = "Prices",
                    DisplayFieldName = "Prices",
                    Aggregations = new Dictionary<FacetAggregation, HashSet<FacetAggregationField>>
                    {
                        { FacetAggregation.Min, new HashSet<FacetAggregationField> { new FacetAggregationField {Name = "Prices" } } }
                    },
                    Options = new FacetOptions
                    {
                        TermSortMode = FacetTermSortMode.ValueAsc
                    }
                },
                new Facet
                {
                    FieldName = "Prices",
                    DisplayFieldName = "PricesMax",
                    //MaxResults = 1,
                    Aggregations = new Dictionary<FacetAggregation, HashSet<FacetAggregationField>>
                    {
                        { FacetAggregation.Max, new HashSet<FacetAggregationField> { new FacetAggregationField {Name = "Prices" } } }
                    },
                    Options = new FacetOptions
                    {
                        TermSortMode = FacetTermSortMode.ValueDesc
                    }
                }
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

            }
        }
    }
}
