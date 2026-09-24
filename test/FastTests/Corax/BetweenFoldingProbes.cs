using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
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
