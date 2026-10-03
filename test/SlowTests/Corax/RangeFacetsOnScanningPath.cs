using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    // A Corax range facet is served either by one range query per range over the index, or by a scan of the matched
    // documents when the facet carries an aggregation or shares the query with a term facet that has more distinct
    // terms than the query has matches. The scan used to read only the first value of a multi-valued field and to
    // compare an integer bound against the truncated long of a double value, so the same facet over the same data
    // produced different counts depending on the path. Every case is held to the counts of the other path and of Lucene.
    public class RangeFacetsOnScanningPath : RavenTestBase
    {
        public RangeFacetsOnScanningPath(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Sku { get; set; }
            public string Category { get; set; }
            public double Price { get; set; }
            public double[] Prices { get; set; }
        }

        private class ItemIndex : AbstractIndexCreationTask<Item>
        {
            public ItemIndex()
            {
                Map = items => from item in items
                               select new { item.Sku, item.Category, item.Price, item.Prices };
            }
        }

        private const int Matches = 10;
        private const double Price = 15.7;

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MultiValuedRangeFacet_SameCountsWithAndWithoutTermFacet(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var alone = Counts(Query(session).AggregateBy(Ranges("Prices < 10", "Prices >= 10")).Execute()["Prices"]);
                    Assert.Equal(Matches, alone["Prices < 10"]);
                    Assert.Equal(Matches, alone["Prices >= 10"]);

                    var nextToTermFacet = Counts(Query(session).AggregateBy(new Facet { FieldName = "Sku" }).AndAggregateBy(Ranges("Prices < 10", "Prices >= 10")).Execute()["Prices"]);
                    Assert.Equal(Matches, nextToTermFacet["Prices < 10"]);
                    Assert.Equal(Matches, nextToTermFacet["Prices >= 10"]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void IntegerLiteralRangeOnDoubleField_SameCountsWithAndWithoutTermFacet(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var alone = Counts(Query(session).AggregateBy(Ranges("Price > 15")).Execute()["Price"]);
                    Assert.Equal(Matches, alone["Price > 15"]);

                    var nextToTermFacet = Counts(Query(session).AggregateBy(new Facet { FieldName = "Sku" }).AndAggregateBy(Ranges("Price > 15")).Execute()["Price"]);
                    Assert.Equal(Matches, nextToTermFacet["Price > 15"]);
                }
            }
        }

        // an aggregated range facet always takes the scan, so this one failed on Corax before the switch existed
        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void AggregatedRangeFacets_ScanningPathCounts(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var prices = Ranges("Prices < 10", "Prices >= 10");
                    prices.Aggregations[FacetAggregation.Sum] = new HashSet<FacetAggregationField> { new FacetAggregationField { Name = nameof(Item.Price) } };
                    var multiValued = Query(session).AggregateBy(prices).Execute()["Prices"];
                    Assert.Equal(Matches, Counts(multiValued)["Prices < 10"]);
                    Assert.Equal(Matches, Counts(multiValued)["Prices >= 10"]);

                    // each matched document is summed once per range it falls into, however many of its values fall into it
                    Assert.Equal(Matches * Price, Sums(multiValued)["Prices < 10"], precision: 6);
                    Assert.Equal(Matches * Price, Sums(multiValued)["Prices >= 10"], precision: 6);

                    var price = Ranges("Price > 15");
                    price.Aggregations[FacetAggregation.Sum] = new HashSet<FacetAggregationField> { new FacetAggregationField { Name = nameof(Item.Price) } };
                    var truncated = Query(session).AggregateBy(price).Execute()["Price"];
                    Assert.Equal(Matches, Counts(truncated)["Price > 15"]);
                    Assert.Equal(Matches * Price, Sums(truncated)["Price > 15"], precision: 6);
                }
            }
        }

        private static IRavenQueryable<Item> Query(IDocumentSession session) => session.Query<Item, ItemIndex>().Where(x => x.Category == "a");

        private static RangeFacet Ranges(params string[] ranges) => new RangeFacet { Ranges = ranges.ToList() };

        private static Dictionary<string, int> Counts(FacetResult result) => result.Values.ToDictionary(x => x.Range, x => x.Count);

        private static Dictionary<string, double> Sums(FacetResult result) => result.Values.ToDictionary(x => x.Range, x => x.Sum.Value);

        // 10 matches in category 'a', 40 distinct skus, so the term facet field has more terms than the query has matches
        private void Seed(IDocumentStore store)
        {
            new ItemIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 40; i++)
                {
                    session.Store(new Item
                    {
                        Sku = $"sku-{i}",
                        Category = i < Matches ? "a" : "b",
                        Price = Price,
                        Prices = new[] { 5.0, 50.0 }
                    });
                }

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }
    }
}
