using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    // Shapes from the review of the between fold, each asserting what the unfolded chain returned before the fold existed.
    // A "twin" writes one bound as '(F < b or F < b)': the fold never descends into 'or', so the twin stays unfolded.
    public class BetweenFoldingProbes : RavenTestBase
    {
        public BetweenFoldingProbes(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Id { get; set; }
            public string Tag { get; set; }
            public DateTime At { get; set; }
            public double Price { get; set; }
            public long[] Values { get; set; }
        }

        private class Items_ByTag : AbstractIndexCreationTask<Item>
        {
            public Items_ByTag()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Tag,
                                   item.At,
                                   item.Price,
                                   item.Values
                               };
            }
        }

        // Lucene splits date ranges into day segments only when the query clause cache is on, and it is off by default
        private class Items_ByTagWithClauseCache : Items_ByTag
        {
            public Items_ByTagWithClauseCache()
            {
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.QueryClauseCacheDisabled)] = "false";
            }
        }

        private class Shipment
        {
            public string Id { get; set; }
            public string Tag { get; set; }
            public Point Origin { get; set; }
            public Point Destination { get; set; }
        }

        private class Point
        {
            public long X { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        // B1 (query clause cache on): 'from' mid-day, 'to' on the next midnight, '<' must exclude the document sitting on 'to'
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void HalfOpenDateRangeEndingOnTheNextMidnight(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 11, 0, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/inside", Tag = "t", At = from.AddHours(1) },
                new Item { Id = "items/on-upper", Tag = "t", At = to });

            using var session = store.OpenSession();
            var linq = session.Query<Item, Items_ByTagWithClauseCache>()
                .Where(x => x.Tag == "t" && x.At >= from && x.At < to);

            Output.WriteLine(linq.ToString());
            Assert.Equal(new[] { "items/inside" }, linq.ToList().Select(x => x.Id).OrderBy(x => x).ToArray());
        }

        // B2 (query clause cache on): 'from' after 'to', on different days, nothing can match
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InvertedDateRangeMatchesNothing(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 5, 12, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/after-from", Tag = "t", At = from.AddHours(1) },
                new Item { Id = "items/before-to", Tag = "t", At = to.AddHours(-6) });

            Assert.Empty(Query(store, "from index 'Items/ByTagWithClauseCache' where (Tag = 't' and At >= $from) and At < $to", ("from", from), ("to", to)));
        }

        // a range spanning several days with both ends mid-day: head, cached middle days and tail, the documents on the
        // day boundaries must be counted exactly once each and the one sitting on 'to' must be excluded
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DateRangeAcrossSeveralDaysWithUnalignedEnds(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 13, 6, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/before", Tag = "t", At = from.AddHours(-1) },
                new Item { Id = "items/on-from", Tag = "t", At = from },
                new Item { Id = "items/first-midnight", Tag = "t", At = new DateTime(2024, 1, 11, 0, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/middle", Tag = "t", At = new DateTime(2024, 1, 12, 15, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/last-midnight", Tag = "t", At = new DateTime(2024, 1, 13, 0, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/on-to", Tag = "t", At = to },
                new Item { Id = "items/after", Tag = "t", At = to.AddHours(1) });

            Assert.Equal(new[] { "items/first-midnight", "items/last-midnight", "items/middle", "items/on-from" },
                Query(store, "from index 'Items/ByTagWithClauseCache' where (Tag = 't' and At >= $from) and At < $to", ("from", from), ("to", to)));
        }

        // B3: an integer lower bound and a fractional upper bound on the same field
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MixedIntegerAndFractionalBounds(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/cheap", Tag = "t", Price = 12.5 },
                new Item { Id = "items/pricey", Tag = "t", Price = 25 });

            Assert.Equal(new[] { "items/cheap" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Price >= 10) and Price < 20.5"));

            // the same through parameters, typed the way a JSON client sends 10 and 20.5
            Assert.Equal(new[] { "items/cheap" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Price >= $lo) and Price < $hi", ("lo", 10), ("hi", 20.5)));
        }

        // B4: two different nested fields that share the last path segment
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangeOnDifferentNestedFieldsWithTheSameLeaf(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new Shipment { Id = "shipments/1", Tag = "t", Origin = new Point { X = 1 }, Destination = new Point { X = 100 } });
                session.SaveChanges();
            }

            // Destination.X = 100 fails 'Destination.X < 5'
            Assert.Empty(QueryDynamic<Shipment>(store, "from Shipments where (Tag = 't' and Origin.X >= 0) and (Destination.X < 5 or Destination.X < 5)"));
            Assert.Empty(QueryDynamic<Shipment>(store, "from Shipments where (Tag = 't' and Origin.X >= 0) and Destination.X < 5"));
        }

        // B5: exact() range pair on an auto index, 'Bob' is in ['A', 'M') case-sensitively, 'alice' is not
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactRangePairOnAutoIndex(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new User { Id = "users/1", Name = "Bob", Age = 30 });
                session.Store(new User { Id = "users/2", Name = "alice", Age = 30 });
                session.SaveChanges();
            }

            Assert.Equal(new[] { "users/1" }, QueryDynamic<User>(store, "from Users where exact((Age >= 0 and Name >= 'A') and (Name < 'M' or Name < 'M'))"));
            Assert.Equal(new[] { "users/1" }, QueryDynamic<User>(store, "from Users where exact((Age >= 0 and Name >= 'A') and Name < 'M')"));
        }

        // B6: multi-valued field, one value above the lower bound and another one below the upper bound.
        // As two clauses this matched, since any element may satisfy each bound on its own. Folded into a between it means
        // "one element inside the range", which is what a hand-written between, the LINQ provider's own between and the
        // old fold of adjacent bounds already meant, and what a range on a collection is read to mean. The fold makes
        // that the meaning regardless of how the two bounds are nested.
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangePairOnMultiValuedField(Options options)
        {
            // the extra documents give each term two entries, a single-entry term trips an unrelated Corax range issue
            using var store = Seed(options,
                new Item { Id = "items/1", Tag = "t", Values = new long[] { 0, 10 } },
                new Item { Id = "items/in-range", Tag = "t", Values = new long[] { 0, 3 } },
                new Item { Id = "items/zero", Tag = "x", Values = new long[] { 0 } },
                new Item { Id = "items/ten", Tag = "x", Values = new long[] { 10 } });

            Assert.Equal(new[] { "items/in-range" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Values >= 1) and Values < 5"));
            Assert.Equal(new[] { "items/in-range" }, Query(store, "from index 'Items/ByTag' where Tag = 't' and Values between 1 and 4"));
        }

        private IDocumentStore Seed(Options options, params Item[] items) => Seed(options, new Items_ByTag(), items);

        private IDocumentStore Seed(Options options, Items_ByTag index, params Item[] items)
        {
            var store = GetDocumentStore(options);
            index.Execute(store);

            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                    session.Store(item);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            return store;
        }

        private static string[] Query(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
        {
            using var session = store.OpenSession();
            var query = session.Advanced.RawQuery<Item>(rql);
            foreach (var (name, value) in parameters)
                query = query.AddParameter(name, value);

            return query.ToList().Select(x => x.Id).OrderBy(x => x).ToArray();
        }

        private static string[] QueryDynamic<T>(IDocumentStore store, string rql)
        {
            using var session = store.OpenSession();
            var results = session.Advanced.RawQuery<T>(rql).WaitForNonStaleResults().ToList();
            return results.Select(x => session.Advanced.GetDocumentId(x)).OrderBy(x => x).ToArray();
        }
    }
}
