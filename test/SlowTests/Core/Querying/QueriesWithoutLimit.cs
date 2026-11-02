using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Core.Querying
{
    // Lucene sizes its sort and score collectors by the requested page size. A query without a limit asks for the
    // whole index, and the collectors used to allocate a fresh array of that size (MaxDoc) on every such query and
    // discard it right after. The Lucene fork now rents that storage from a pool and returns it when the query
    // completes, so these tests pin the behavior of every path that relies on it: sorted queries (one and many
    // comparators, dates, numbers and strings), skip without take, no matches, no where clause, and boosted
    // queries with and without the automatic order by score.
    public class QueriesWithoutLimit : RavenTestBase
    {
        public QueriesWithoutLimit(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void OrderByWithoutLimit_ReturnsEveryMatchInOrder(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var expected = orders.Where(x => x.Company == "A").ToList();
                Assert.InRange(expected.Count, 2, orders.Count / 10); // the fix only matters when few documents match

                using (var session = store.OpenSession())
                {
                    // a date field is sorted by its ticks (a LONG sort on the _Time field)
                    var byDate = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "A")
                        .OrderByDescending(x => x.CreatedAt)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected.OrderByDescending(x => x.CreatedAt).Select(x => x.Id), byDate.Select(x => x.Id));

                    // a numeric field is a LONG sort on the _L_Range field
                    var byAmount = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out stats)
                        .Where(x => x.Company == "A")
                        .OrderBy(x => x.Amount)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected.OrderBy(x => x.Amount).Select(x => x.Amount), byAmount.Select(x => x.Amount));
                    Assert.Equal(expected.Select(x => x.Id).OrderBy(x => x), byAmount.Select(x => x.Id).OrderBy(x => x));

                    // a string field uses the ordinal string comparator
                    var byName = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out stats)
                        .Where(x => x.Company == "A")
                        .OrderBy(x => x.Name)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected.OrderBy(x => x.Name, StringComparer.Ordinal).Select(x => x.Id), byName.Select(x => x.Id));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void OrderByManyFieldsWithoutLimit_ReturnsEveryMatchInOrder(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var expected = orders.Where(x => x.Company == "A" || x.Company == "C").ToList();

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "A" || x.Company == "C")
                        .OrderBy(x => x.Company)
                        .ThenByDescending(x => x.CreatedAt)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(
                        expected.OrderBy(x => x.Company, StringComparer.Ordinal).ThenByDescending(x => x.CreatedAt).Select(x => x.Id),
                        results.Select(x => x.Id));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void SkipWithoutTake_PagesOverEveryMatch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var expected = orders.Where(x => x.Company == "A").OrderByDescending(x => x.CreatedAt).ToList();
                var skip = expected.Count / 2;

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "A")
                        .OrderByDescending(x => x.CreatedAt)
                        .Skip(skip)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected.Skip(skip).Select(x => x.Id), results.Select(x => x.Id));

                    // skipping past the end still reports the total and returns nothing
                    results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out stats)
                        .Where(x => x.Company == "A")
                        .OrderByDescending(x => x.CreatedAt)
                        .Skip(expected.Count + 5)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Empty(results);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void OrderByWithoutLimit_NoMatches(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                SeedOrders(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == "no-such-company")
                        .OrderBy(x => x.Name)
                        .ToList();

                    Assert.Empty(results);
                    Assert.Equal(0, stats.TotalResults);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void OrderByWithoutWhereOrLimit_ReturnsEveryLiveDocumentInOrder(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);

                // leave deleted documents behind so the live document count differs from the size of the index
                var deleted = orders.Where((_, i) => i % 100 == 0).ToList();
                using (var session = store.OpenSession())
                {
                    foreach (var order in deleted)
                        session.Delete(order.Id);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                var expected = orders.Except(deleted).ToList();

                using (var session = store.OpenSession())
                {
                    // no where clause: the query matches every live document
                    var results = session.Query<Order, Orders_ByCompany>()
                        .Statistics(out var stats)
                        .OrderByDescending(x => x.CreatedAt)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected.OrderByDescending(x => x.CreatedAt).Select(x => x.Id), results.Select(x => x.Id));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void BoostedIndexWithoutWhereOrLimit_ReturnsEveryLiveDocument(Options options)
        {
            // an index with a boosted field and no order by takes the score collector; without a where clause every
            // live document is a hit
            options.ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();

            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                new Orders_ByCompany_Boosted().Execute(store);

                var deleted = orders.Where((_, i) => i % 50 == 0).ToList();
                using (var session = store.OpenSession())
                {
                    foreach (var order in deleted)
                        session.Delete(order.Id);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                var expected = orders.Except(deleted).Select(x => x.Id).ToHashSet();

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Order, Orders_ByCompany_Boosted>()
                        .Statistics(out var stats)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected, results.Select(x => x.Id).ToHashSet());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void BoostedQueryWithoutLimit_OrderedByScoreAutomatically(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var expectedA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();
                var expectedC = orders.Where(x => x.Company == "C").Select(x => x.Id).ToHashSet();

                using (var session = store.OpenSession())
                {
                    var results = BoostedQuery(session, out var stats);

                    Assert.Equal(expectedA.Count + expectedC.Count, stats.TotalResults);
                    // the boosted clause scores higher, so with the automatic order by score every A comes before every C
                    Assert.Equal(expectedA, results.Take(expectedA.Count).Select(x => x.Id).ToHashSet());
                    Assert.Equal(expectedC, results.Skip(expectedA.Count).Select(x => x.Id).ToHashSet());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void BoostedQueryWithoutLimit_WithoutAutomaticOrderByScore(Options options)
        {
            options.ModifyDatabaseRecord = record =>
                record.Settings[RavenConfiguration.GetKey(i => i.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();

            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var expected = orders.Where(x => x.Company == "A" || x.Company == "C").Select(x => x.Id).ToHashSet();

                using (var session = store.OpenSession())
                {
                    var results = BoostedQuery(session, out var stats);

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(expected, results.Select(x => x.Id).ToHashSet());
                }
            }
        }

        private static List<Order> BoostedQuery(IDocumentSession session, out QueryStatistics stats)
        {
            return session.Advanced.DocumentQuery<Order, Orders_ByCompany>()
                .Statistics(out stats)
                .WhereEquals(x => x.Company, "A").Boost(5)
                .OrElse()
                .WhereEquals(x => x.Company, "C")
                .ToList();
        }

        private List<Order> SeedOrders(IDocumentStore store)
        {
            new Orders_ByCompany().Execute(store);

            var baseDate = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var orders = new List<Order>();
            for (var i = 0; i < 1000; i++)
            {
                orders.Add(new Order
                {
                    Id = $"orders/{i}",
                    Company = i % 27 == 0 ? "A" : i % 29 == 0 ? "C" : "B",
                    // 7 and 1000 are coprime, so the dates are distinct and the matches are out of insertion order
                    CreatedAt = baseDate.AddMinutes(i * 7 % 1000),
                    Amount = i * 31 % 997,
                    Name = $"order-{i * 13 % 1000:D4}"
                });
            }

            using (var bulk = store.BulkInsert())
            {
                foreach (var order in orders)
                    bulk.Store(order);
            }

            Indexes.WaitForIndexing(store);
            return orders;
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public DateTime CreatedAt { get; set; }
            public long Amount { get; set; }
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
                                    o.CreatedAt,
                                    o.Amount,
                                    o.Name
                                };
            }
        }

        // a boosted field makes the Lucene read operation treat every query on the index as boosted, which takes the
        // score collector path when nothing asks for an order
        private class Orders_ByCompany_Boosted : AbstractIndexCreationTask<Order>
        {
            public Orders_ByCompany_Boosted()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    Company = o.Company.Boost(2),
                                    o.CreatedAt
                                };
            }
        }
    }
}
