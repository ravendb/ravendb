using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Core.Querying
{
    // A Lucene query without an order by used to walk every matching index entry just to report the total, even for
    // a count-only query or the first page of a broad query. A match-all query and a single term query now take their
    // total from the index statistics (the live document count of a segment, the term's document frequency of a
    // segment without deletions) and walk only what the page needs. These tests pin the totals and the pages against
    // the documents, on an index written in several batches, before and after deletes and updates leave deleted
    // entries behind in the segments, and on every index shape whose query becomes such a term query.
    public class CountingFromIndexStatistics : RavenTestBase
    {
        public CountingFromIndexStatistics(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_MatchTheDocuments_BeforeAndAfterDeletesAndUpdates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                AssertCountsAndPages(store, orders);

                // deletes and updates leave deleted entries in the Lucene segments until they are merged, which is
                // when a term's document frequency stops being the number of live matches
                using (var session = store.OpenSession())
                {
                    foreach (var order in orders.Where((_, i) => i % 10 == 0).ToList())
                    {
                        session.Delete(order.Id);
                        orders.Remove(order);
                    }

                    foreach (var order in orders.Where((_, i) => i % 7 == 0))
                    {
                        order.Company = order.Company == "A" ? "C" : "A";
                        session.Store(order);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                AssertCountsAndPages(store, orders);
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_NoMatches_NullAndEmptyValues(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);

                // a null and an empty string are indexed as marker terms, so their equality queries are term queries too
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 20; i++)
                        session.Store(new Order { Id = $"orders/null/{i}", Company = null, Name = $"null-{i}" });

                    for (var i = 0; i < 30; i++)
                        session.Store(new Order { Id = $"orders/empty/{i}", Company = "", Name = $"empty-{i}" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "no-such-company"));
                    Assert.Equal(20, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == null));
                    Assert.Equal(30, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == ""));
                    Assert.Equal(orders.Count + 50, session.Query<Order, Orders_ByCompany>().Count());

                    AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "no-such-company").Take(10), new HashSet<string>(), 0);
                    AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == null).Take(10), Enumerable.Range(0, 20).Select(i => $"orders/null/{i}").ToHashSet(), 10);
                    AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "").Take(10), Enumerable.Range(0, 30).Select(i => $"orders/empty/{i}").ToHashSet(), 10);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_OnAnAutoIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store, createIndex: false);
                var companyA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();

                using (var session = store.OpenSession())
                {
                    Assert.Equal(companyA.Count, session.Query<Order>().Count(x => x.Company == "A"));
                    Assert.Equal(0, session.Query<Order>().Count(x => x.Company == "no-such-company"));

                    AssertPage(session.Query<Order>().Where(x => x.Company == "A").Take(10), companyA, 10);
                    AssertPage(session.Query<Order>().Where(x => x.Company == "A").Skip(companyA.Count - 3).Take(10), companyA, 3);
                }

                Assert.Contains(store.Maintenance.Send(new Raven.Client.Documents.Operations.Indexes.GetIndexNamesOperation(0, 10)), x => x.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase));
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void TotalWithoutResults_WhenStartIsPastZeroAndPageSizeIsZero(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var companyA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();

                // start without a page size is not a count query, the collector still has to walk the skipped hits
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<Order, Orders_ByCompany>()
                        .WhereEquals(x => x.Company, "A")
                        .Statistics(out var stats)
                        .Skip(5)
                        .Take(0)
                        .ToList();

                    Assert.Empty(results);
                    Assert.Equal(companyA.Count, stats.TotalResults);

                    results = session.Advanced.DocumentQuery<Order, Orders_ByCompany>()
                        .Statistics(out stats)
                        .Skip(orders.Count + 10)
                        .Take(0)
                        .ToList();

                    Assert.Empty(results);
                    Assert.Equal(orders.Count, stats.TotalResults);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_OnAFanoutIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Orders_ByLineProduct().Execute(store);
                var orders = SeedOrders(store, createIndex: false);
                var entries = orders.Sum(x => x.Lines.Count);
                var withProductP2 = orders.Where(x => x.Lines.Any(l => l.Product == "p2")).Select(x => x.Id).ToHashSet();

                using (var session = store.OpenSession())
                {
                    // the total of a fanout index is the number of index entries, as it always was
                    Assert.Equal(entries, session.Query<Order, Orders_ByLineProduct>().Count());
                    Assert.Equal(orders.Count, session.Query<Order, Orders_ByLineProduct>().Count(x => x.Lines.Any(l => l.Product == "p0")));
                    Assert.Equal(withProductP2.Count, session.Query<Order, Orders_ByLineProduct>().Count(x => x.Lines.Any(l => l.Product == "p2")));

                    // a page is cut by document, so the server walks past the page's entries to fill it
                    AssertPage(session.Query<Order, Orders_ByLineProduct>().Take(10), orders.Select(x => x.Id).ToHashSet(), 10, entries);
                    AssertPage(session.Query<Order, Orders_ByLineProduct>().Where(x => x.Lines.Any(l => l.Product == "p2")).Take(10), withProductP2, 10);
                }
            }
        }

        // a single database only: the sharded orchestrator sums the totals of the shards, so a group that every
        // shard reduces counts once per shard there, with or without this change
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CountsAndPages_OnAMapReduceIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Orders_CountByCompany().Execute(store);
                var orders = SeedOrders(store, createIndex: false);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(3, session.Query<Orders_CountByCompany.Result, Orders_CountByCompany>().Count());
                    Assert.Equal(1, session.Query<Orders_CountByCompany.Result, Orders_CountByCompany>().Count(x => x.Company == "A"));
                    Assert.Equal(0, session.Query<Orders_CountByCompany.Result, Orders_CountByCompany>().Count(x => x.Company == "no-such-company"));

                    var results = session.Query<Orders_CountByCompany.Result, Orders_CountByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "A")
                        .Take(10)
                        .ToList();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(orders.Count(x => x.Company == "A"), results.Single().Count);
                }
            }
        }

        private static void AssertCountsAndPages(IDocumentStore store, List<Order> orders)
        {
            var all = orders.Select(x => x.Id).ToHashSet();
            var companyA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();
            var companyAOrC = orders.Where(x => x.Company == "A" || x.Company == "C").Select(x => x.Id).ToHashSet();

            using (var session = store.OpenSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue; // paging through every match is many queries

                // count-only queries: no where clause, a single term, and shapes that still have to walk
                Assert.Equal(orders.Count, session.Query<Order, Orders_ByCompany>().Count());
                Assert.Equal(companyA.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A"));
                Assert.Equal(companyAOrC.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A" || x.Company == "C"));
                Assert.Equal(orders.Count - companyA.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company != "A"));

                // a single token on a full text search field is a term query as well
                Assert.Equal(orders.Count, session.Query<Order, Orders_ByCompany>().Search(x => x.Name, "order").Count());

                // a page without an order by still reports the total of every match
                AssertPage(session.Query<Order, Orders_ByCompany>().Take(10), all, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(5).Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count - 3).Take(10), companyA, 3);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count + 5).Take(10), companyA, 0);

                // a projection takes the same page
                var projected = session.Query<Order, Orders_ByCompany>()
                    .Statistics(out var stats)
                    .Where(x => x.Company == "A")
                    .Select(x => x.Name)
                    .Take(10)
                    .ToList();

                Assert.Equal(companyA.Count, stats.TotalResults);
                Assert.Equal(10, projected.Count);
                Assert.All(projected, name => Assert.Contains(name, orders.Where(x => x.Company == "A").Select(x => x.Name)));

                // paging through every match without an order by visits each document exactly once, which holds only
                // if every page is cut from the same walk order
                AssertPagingVisitsEveryDocumentOnce(session, x => x.Company == "A", companyA, 7);
                AssertPagingVisitsEveryDocumentOnce(session, null, all, 97);
            }
        }

        private static void AssertPage(IRavenQueryable<Order> query, HashSet<string> expected, int expectedCount, int? expectedTotal = null)
        {
            var results = query.Statistics(out var stats).ToList();

            Assert.Equal(expectedTotal ?? expected.Count, stats.TotalResults);
            Assert.Equal(expectedCount, results.Count);
            Assert.All(results, x => Assert.Contains(x.Id, expected));
            Assert.Equal(results.Count, results.Select(x => x.Id).Distinct().Count());
        }

        private static void AssertPagingVisitsEveryDocumentOnce(IDocumentSession session, Expression<Func<Order, bool>> where, HashSet<string> expected, int pageSize)
        {
            var seen = new List<string>();
            for (var start = 0; start < expected.Count; start += pageSize)
            {
                var query = session.Query<Order, Orders_ByCompany>();
                if (where != null)
                    query = query.Where(where);

                var page = query.Statistics(out var stats).Skip(start).Take(pageSize).ToList();

                Assert.Equal(expected.Count, stats.TotalResults);
                seen.AddRange(page.Select(x => x.Id));
            }

            Assert.Equal(expected.Count, seen.Count);
            Assert.Equal(expected, seen.ToHashSet());
        }

        private List<Order> SeedOrders(IDocumentStore store, bool createIndex = true)
        {
            if (createIndex)
                new Orders_ByCompany().Execute(store);

            var orders = new List<Order>();
            for (var i = 0; i < 3000; i++)
            {
                orders.Add(new Order
                {
                    Id = $"orders/{i}",
                    Company = i % 11 == 0 ? "A" : i % 13 == 0 ? "C" : "B",
                    Name = $"order {i:D4}",
                    Lines = Enumerable.Range(0, i % 3 + 1).Select(j => new OrderLine { Product = $"p{j}" }).ToList()
                });
            }

            // several batches with indexing in between, so the index has more than one segment
            foreach (var batch in orders.Chunk(1000))
            {
                using (var bulk = store.BulkInsert())
                {
                    foreach (var order in batch)
                        bulk.Store(order);
                }

                Indexes.WaitForIndexing(store);
            }

            return orders;
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Name { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
        }

        private class Orders_ByCompany : AbstractIndexCreationTask<Order>
        {
            public Orders_ByCompany()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    o.Company,
                                    o.Name
                                };

                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        private class Orders_ByLineProduct : AbstractIndexCreationTask<Order>
        {
            public Orders_ByLineProduct()
            {
                Map = orders => from o in orders
                                from line in o.Lines
                                select new
                                {
                                    Lines_Product = line.Product
                                };
            }
        }

        private class Orders_CountByCompany : AbstractIndexCreationTask<Order, Orders_CountByCompany.Result>
        {
            public class Result
            {
                public string Company { get; set; }
                public int Count { get; set; }
            }

            public Orders_CountByCompany()
            {
                Map = orders => from o in orders
                                select new Result
                                {
                                    Company = o.Company,
                                    Count = 1
                                };

                Reduce = results => from r in results
                                    group r by r.Company
                                    into g
                                    select new Result
                                    {
                                        Company = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}
