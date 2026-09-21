using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    // Lucene facets used to intersect every distinct term of the facet field with the query matches, a full pass over the
    // field's terms on every query. When the matches are fewer than the terms the server now walks the matches and reads
    // their terms instead. Both engines are held to the same in-memory expectation for a query that takes the new path,
    // one that takes the old one, and one with no matches at all.
    public class FacetsOverHighCardinalityFields : RavenTestBase
    {
        public FacetsOverHighCardinalityFields(ITestOutputHelper output) : base(output)
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

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Lucene | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void FewerMatchesThanDistinctTerms(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var live = Seed(store);

                // 60 customers out of 1000, one group out of three: a few dozen matches against a thousand distinct terms
                var customerIds = Enumerable.Range(1, 60).Select(i => $"customers/{i}").ToArray();

                using (var session = store.OpenSession())
                {
                    var facets = session.Query<Coin, CoinIndex>()
                        .Where(x => x.Group == "g1" && x.CustomerId.In(customerIds))
                        .AggregateBy(f => f.ByField(x => x.CustomerId).SumOn(x => x.Amount).MinOn(x => x.Amount).MaxOn(x => x.Amount).AverageOn(x => x.Amount))
                        .AndAggregateBy(f => f.ByField(x => x.Tags))
                        .AndAggregateBy(f => f.ByField(x => x.Group))
                        .Execute();

                    var expected = live.Where(x => x.Group == "g1" && customerIds.Contains(x.CustomerId)).ToList();
                    Assert.InRange(expected.Count, 1, CustomerCount - 1);

                    AssertFacets(facets, expected);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Lucene | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MoreMatchesThanDistinctTerms(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var live = Seed(store);

                var groups = new[] { "g0", "g1", "g2" };

                using (var session = store.OpenSession())
                {
                    var facets = session.Query<Coin, CoinIndex>()
                        .Where(x => x.Group.In(groups))
                        .AggregateBy(f => f.ByField(x => x.CustomerId).SumOn(x => x.Amount).MinOn(x => x.Amount).MaxOn(x => x.Amount).AverageOn(x => x.Amount))
                        .AndAggregateBy(f => f.ByField(x => x.Tags))
                        .AndAggregateBy(f => f.ByField(x => x.Group))
                        .Execute();

                    Assert.True(live.Count > CustomerCount, "the whole data set has to outnumber the distinct customers");

                    AssertFacets(facets, live);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Lucene | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NoMatches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var facets = session.Query<Coin, CoinIndex>()
                        .Where(x => x.Group == "none")
                        .AggregateBy(f => f.ByField(x => x.CustomerId).SumOn(x => x.Amount))
                        .AndAggregateBy(f => f.ByField(x => x.Tags))
                        .Execute();

                    Assert.Empty(facets[nameof(Coin.CustomerId)].Values);
                    Assert.Empty(facets[nameof(Coin.Tags)].Values);
                }
            }
        }

        // three batches so Lucene gets more than one segment, then deletions so the postings carry deleted documents
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

        private static void AssertFacets(Dictionary<string, FacetResult> facets, List<Coin> expected)
        {
            var byCustomer = expected.GroupBy(x => x.CustomerId).ToDictionary(g => g.Key, g => g.ToList());
            var customers = facets[nameof(Coin.CustomerId)].Values;

            Assert.Equal(byCustomer.Count, customers.Count);
            foreach (var value in customers)
            {
                Assert.True(byCustomer.TryGetValue(value.Range, out var docs), $"unexpected customer '{value.Range}' in the facet");

                var amounts = docs.Select(x => (double)x.Amount).ToList();
                Assert.Equal(docs.Count, value.Count);
                Assert.Equal(amounts.Sum(), value.Sum.Value, 6);
                Assert.Equal(amounts.Min(), value.Min.Value, 6);
                Assert.Equal(amounts.Max(), value.Max.Value, 6);
                Assert.Equal(amounts.Average(), value.Average.Value, 6);
            }

            var byTag = expected.SelectMany(x => x.Tags).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count());
            var tags = facets[nameof(Coin.Tags)].Values;

            Assert.Equal(byTag.Count, tags.Count);
            foreach (var value in tags)
            {
                Assert.True(byTag.TryGetValue(value.Range, out var count), $"unexpected tag '{value.Range}' in the facet");
                Assert.Equal(count, value.Count);
            }

            var byGroup = expected.GroupBy(x => x.Group).ToDictionary(g => g.Key, g => g.Count());
            var groups = facets[nameof(Coin.Group)].Values;

            Assert.Equal(byGroup.Count, groups.Count);
            foreach (var value in groups)
            {
                Assert.True(byGroup.TryGetValue(value.Range, out var count), $"unexpected group '{value.Range}' in the facet");
                Assert.Equal(count, value.Count);
            }
        }
    }
}
