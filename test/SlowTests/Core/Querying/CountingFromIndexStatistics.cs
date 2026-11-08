using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Core.Querying
{
    // A Lucene query without an order by used to walk every matching index entry just to report the total, even for
    // a count-only query or the first page of a broad query. A match-all query and a single term query now take their
    // total from the index statistics (the live document count of a segment, the term's document frequency of a
    // segment without deletions) and walk only what the page needs. These tests pin the totals and the pages against
    // the documents, on an index written in several batches, before and after deletes and updates leave deleted
    // entries behind in the segments.
    public class CountingFromIndexStatistics : RavenTestBase
    {
        public CountingFromIndexStatistics(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
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
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CountQuery_NoMatchesAndMissingField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                SeedOrders(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "no-such-company"));

                    var results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "no-such-company")
                        .Take(10)
                        .ToList();

                    Assert.Empty(results);
                    Assert.Equal(0, stats.TotalResults);
                }
            }
        }

        private static void AssertCountsAndPages(IDocumentStore store, List<Order> orders)
        {
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

                // a page without an order by still reports the total of every match
                AssertPage(session.Query<Order, Orders_ByCompany>().Take(10), orders.Select(x => x.Id).ToHashSet(), 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(5).Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count - 3).Take(10), companyA, 3);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count + 5).Take(10), companyA, 0);

                // paging through every match without an order by visits each document exactly once, which holds only
                // if every page is cut from the same walk order
                AssertPagingVisitsEveryDocumentOnce(session, x => x.Company == "A", companyA, 7);
                AssertPagingVisitsEveryDocumentOnce(session, null, orders.Select(x => x.Id).ToHashSet(), 97);
            }
        }

        private static void AssertPage(IRavenQueryable<Order> query, HashSet<string> expected, int expectedCount)
        {
            var results = query.Statistics(out var stats).ToList();

            Assert.Equal(expected.Count, stats.TotalResults);
            Assert.Equal(expectedCount, results.Count);
            Assert.All(results, x => Assert.Contains(x.Id, expected));
            Assert.Equal(results.Count, results.Select(x => x.Id).Distinct().Count());
        }

        private static void AssertPagingVisitsEveryDocumentOnce(Raven.Client.Documents.Session.IDocumentSession session, System.Linq.Expressions.Expression<System.Func<Order, bool>> where, HashSet<string> expected, int pageSize)
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

        private List<Order> SeedOrders(IDocumentStore store)
        {
            new Orders_ByCompany().Execute(store);

            var orders = new List<Order>();
            for (var i = 0; i < 3000; i++)
            {
                orders.Add(new Order
                {
                    Id = $"orders/{i}",
                    Company = i % 11 == 0 ? "A" : i % 13 == 0 ? "C" : "B",
                    Name = $"order-{i:D4}"
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
            }
        }
    }
}
