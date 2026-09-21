using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    // A Corax count-only facet with a where clause used to run one term query per distinct term of the facet field,
    // however few documents matched. When the matches are fewer than the terms the server now takes the per-document
    // scan it already used for aggregated facets. Count-only facets are held to an in-memory expectation for a query
    // that takes the scan, one that keeps the per-term path, and one with no matches, and the two paths are checked
    // against each other through the aggregated variant of the same facet.
    public class CountFacetsOverHighCardinalityFields : RavenTestBase
    {
        public CountFacetsOverHighCardinalityFields(ITestOutputHelper output) : base(output)
        {
        }

        private class Coin
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string Group { get; set; }
            public string[] Tags { get; set; }
            public decimal Amount { get; set; }
        }

        private class CoinIndex : AbstractIndexCreationTask<Coin>
        {
            public CoinIndex()
            {
                Map = coins => from coin in coins
                               select new
                               {
                                   coin.CustomerId,
                                   coin.Group,
                                   coin.Tags,
                                   coin.Amount
                               };
            }
        }

        private const int DocumentCount = 1200;
        private const int CustomerCount = 1000;

        private static Coin Create(int i) => new Coin
        {
            Id = $"coins/{i}",
            CustomerId = $"customers/{i % CustomerCount}",
            Group = $"g{i % 3}",
            Tags = new[] { $"tag-a{i % 5}", $"tag-b{i % 7}" },
            Amount = (i % 97) + 0.5m
        };

        private static bool IsDeleted(int i) => i % 50 == 0;

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void FewerMatchesThanDistinctTerms(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var live = Seed(store);

                // 60 customers out of 1000, one group out of three: a few dozen matches against a thousand distinct terms
                var customerIds = Enumerable.Range(1, 60).Select(i => $"customers/{i}").ToArray();

                using (var session = store.OpenSession())
                {
                    var facets = CountFacets(session.Query<Coin, CoinIndex>().Where(x => x.Group == "g1" && x.CustomerId.In(customerIds)));

                    var expected = live.Where(x => x.Group == "g1" && customerIds.Contains(x.CustomerId)).ToList();
                    Assert.InRange(expected.Count, 1, CustomerCount - 1);

                    AssertCounts(facets, expected);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void MoreMatchesThanDistinctTerms(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var live = Seed(store);

                var groups = new[] { "g0", "g1", "g2" };

                using (var session = store.OpenSession())
                {
                    var facets = CountFacets(session.Query<Coin, CoinIndex>().Where(x => x.Group.In(groups)));

                    Assert.True(live.Count > CustomerCount, "the whole data set has to outnumber the distinct customers");

                    AssertCounts(facets, live);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void NoMatches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var facets = CountFacets(session.Query<Coin, CoinIndex>().Where(x => x.Group == "none"));

                    Assert.Empty(facets[nameof(Coin.CustomerId)].Values);
                    Assert.Empty(facets[nameof(Coin.Tags)].Values);
                    Assert.Empty(facets[nameof(Coin.Group)].Values);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CountOnlyAndAggregatedFacetsAgree(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                var groups = new[] { "g0", "g1", "g2" };
                var customerIds = Enumerable.Range(1, 60).Select(i => $"customers/{i}").ToArray();

                using (var session = store.OpenSession())
                {
                    // the aggregated facet always scans the matched documents; the count-only one picks its path by the match count
                    foreach (var query in new[]
                    {
                        session.Query<Coin, CoinIndex>().Where(x => x.Group == "g1" && x.CustomerId.In(customerIds)),
                        session.Query<Coin, CoinIndex>().Where(x => x.Group.In(groups))
                    })
                    {
                        var countOnly = query.AggregateBy(f => f.ByField(x => x.CustomerId)).Execute()[nameof(Coin.CustomerId)];
                        var aggregated = query.AggregateBy(f => f.ByField(x => x.CustomerId).SumOn(x => x.Amount)).Execute()[nameof(Coin.CustomerId)];

                        Assert.NotEmpty(countOnly.Values);
                        Assert.Equal(
                            aggregated.Values.OrderBy(x => x.Range).Select(x => (x.Range, x.Count)),
                            countOnly.Values.OrderBy(x => x.Range).Select(x => (x.Range, x.Count)));
                    }
                }
            }
        }

        private static Dictionary<string, FacetResult> CountFacets(IRavenQueryable<Coin> query)
        {
            return query
                .AggregateBy(f => f.ByField(x => x.CustomerId))
                .AndAggregateBy(f => f.ByField(x => x.Tags))
                .AndAggregateBy(f => f.ByField(x => x.Group))
                .Execute();
        }

        // three batches, then deletions, so the index holds more than one write and the postings carry deleted documents
        private List<Coin> Seed(IDocumentStore store)
        {
            new CoinIndex().Execute(store);

            const int batchSize = DocumentCount / 3;
            for (var batch = 0; batch < 3; batch++)
            {
                using (var session = store.OpenSession())
                {
                    for (var i = batch * batchSize; i < (batch + 1) * batchSize; i++)
                        session.Store(Create(i));

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < DocumentCount; i++)
                {
                    if (IsDeleted(i))
                        session.Delete($"coins/{i}");
                }

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            return Enumerable.Range(0, DocumentCount).Where(i => IsDeleted(i) == false).Select(Create).ToList();
        }

        private static void AssertCounts(Dictionary<string, FacetResult> facets, List<Coin> expected)
        {
            AssertCounts(facets[nameof(Coin.CustomerId)], expected.GroupBy(x => x.CustomerId).ToDictionary(g => g.Key, g => g.Count()));
            AssertCounts(facets[nameof(Coin.Tags)], expected.SelectMany(x => x.Tags).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
            AssertCounts(facets[nameof(Coin.Group)], expected.GroupBy(x => x.Group).ToDictionary(g => g.Key, g => g.Count()));
        }

        private static void AssertCounts(FacetResult facet, Dictionary<string, int> expected)
        {
            Assert.Equal(expected.Count, facet.Values.Count);
            foreach (var value in facet.Values)
            {
                Assert.True(expected.TryGetValue(value.Range, out var count), $"unexpected term '{value.Range}' in facet '{facet.Name}'");
                Assert.Equal(count, value.Count);
            }
        }
    }
}
