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
    // A Corax query used to read every remaining matching entry just to report the total, for a count-only query and
    // after the page of any query that does not skip statistics. A single term match and the memoized all-entries match
    // of a query without a where clause know their exact number of entries, so the total now comes from them. Composite
    // matches report only an estimate (an AND reports its smaller side, an OR the sum of both), so they keep reading,
    // and so do sorted and distinct queries. These tests pin the totals and the pages against the documents, on an index
    // written in several batches, before and after deletes and updates, for the shapes that take the shortcut and for
    // those that must not, and on every index shape whose query becomes such a match.
    public class CountingFromCoraxMatchCount : RavenTestBase
    {
        public CountingFromCoraxMatchCount(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_MatchTheDocuments_BeforeAndAfterDeletesAndUpdates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                AssertCountsAndPages(store, orders);

                // Corax removes deleted and updated entries from the posting lists and from the index's entry count
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
                        order.Amount += 1;
                        session.Store(order);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                AssertCountsAndPages(store, orders);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_NoMatches_NullAndEmptyValues_EmptyIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Orders_ByCompany().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<Order, Orders_ByCompany>().Count());
                    Assert.Equal(0, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A"));
                }

                var orders = SeedOrders(store);

                // a null and an empty string are indexed as terms of their own, so their equality queries are term matches too
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 20; i++)
                        session.Store(new Order { Id = $"orders/null/{i}", Company = null, Name = $"null {i}" });

                    for (var i = 0; i < 30; i++)
                        session.Store(new Order { Id = $"orders/empty/{i}", Company = "", Name = $"empty {i}" });

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

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
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

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void TotalWithoutResults_WhenStartIsPastZeroAndPageSizeIsZero(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);
                var companyA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();

                // start without a page size is not a count query
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

        // A fanout index writes one entry per line, so a document can have several entries, or none. The total counts
        // entries, both when it is read from the match and when every entry is read. Each total that takes the shortcut is
        // compared with the same query written so that it has to read every entry, before and after updates that grow,
        // shrink and empty the fanout of a document.
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_OnAFanoutIndex_BeforeAndAfterUpdatesThatChangeTheFanout(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Orders_ByLineProduct().Execute(store);
                var orders = SeedOrders(store, createIndex: false);
                AssertFanoutCountsAndPages(store, orders);

                using (var session = store.OpenSession())
                {
                    foreach (var order in orders.Where((_, i) => i % 9 == 0).ToList())
                    {
                        session.Delete(order.Id);
                        orders.Remove(order);
                    }

                    foreach (var (order, i) in orders.Select((o, i) => (o, i)).Where(x => x.i % 4 == 0))
                    {
                        var lines = (i / 4) % 3 == 0 ? 6 : (i / 4) % 3 == 1 ? 1 : 0;
                        order.Lines = Enumerable.Range(0, lines).Select(j => new OrderLine { Product = $"p{(i + j) % 4}" }).ToList();
                        order.Company = order.Company == "A" ? "C" : "A";
                        session.Store(order);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                AssertFanoutCountsAndPages(store, orders);
            }
        }

        // An entry can hold several values of one field, and the same value more than once. A term counts the entries
        // that hold it, each once.
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_OnAMultiValuedFieldWithRepeatedValues_BeforeAndAfterUpdates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Orders_ByProducts().Execute(store);
                var orders = SeedOrdersWithRepeatedProducts(store);
                AssertMultiValuedCountsAndPages(store, orders);

                using (var session = store.OpenSession())
                {
                    foreach (var (order, i) in orders.Select((o, i) => (o, i)).Where(x => x.i % 5 == 0))
                    {
                        order.Lines = Enumerable.Range(0, (i / 5) % 3 * 2).Select(_ => new OrderLine { Product = "p1" }).ToList();
                        session.Store(order);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                AssertMultiValuedCountsAndPages(store, orders);
            }
        }

        // A multi-map index writes entries from two collections into one index, and its totals count both.
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void CountsAndPages_OnAMultiMapIndex_BeforeAndAfterDeletesAndUpdates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new OrdersAndInvoices_ByCompany().Execute(store);
                var orders = SeedOrders(store, createIndex: false);
                var invoices = Enumerable.Range(0, 500).Select(i => new Invoice { Id = $"invoices/{i}", Company = i % 7 == 0 ? "A" : "D" }).ToList();

                using (var bulk = store.BulkInsert())
                {
                    foreach (var invoice in invoices)
                        bulk.Store(invoice);
                }

                Indexes.WaitForIndexing(store);
                AssertMultiMapCountsAndPages(store, orders, invoices);

                using (var session = store.OpenSession())
                {
                    foreach (var order in orders.Where((_, i) => i % 6 == 0).ToList())
                    {
                        session.Delete(order.Id);
                        orders.Remove(order);
                    }

                    foreach (var invoice in invoices.Where((_, i) => i % 5 == 0))
                    {
                        invoice.Company = invoice.Company == "A" ? "D" : "A";
                        session.Store(invoice);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                AssertMultiMapCountsAndPages(store, orders, invoices);
            }
        }

        // a single database only: the sharded orchestrator sums the totals of the shards, so a group that every
        // shard reduces counts once per shard there, with or without this change
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
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

        // a single database only: a distinct count is not supported on a sharded database
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void DistinctCount_StillReadsEveryEntry(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var orders = SeedOrders(store);

                // a distinct count removes the duplicates it reads, so it never takes the match's count
                using (var session = store.OpenSession())
                {
                    Assert.Equal(3, session.Query<Order, Orders_ByCompany>().Select(x => x.Company).Distinct().Count());
                    Assert.Equal(1, session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Select(x => x.Company).Distinct().Count());
                    Assert.Equal(orders.Select(x => x.Amount).Distinct().Count(), session.Query<Order, Orders_ByCompany>().Select(x => x.Amount).Distinct().Count());
                }
            }
        }

        private static void AssertCountsAndPages(IDocumentStore store, List<Order> orders)
        {
            var all = orders.Select(x => x.Id).ToHashSet();
            var companyA = orders.Where(x => x.Company == "A").Select(x => x.Id).ToHashSet();
            var companyAOrC = orders.Where(x => x.Company == "A" || x.Company == "C").Select(x => x.Id).ToHashSet();
            var companyAAndAmount7 = orders.Where(x => x.Company == "A" && x.Amount == 7).Select(x => x.Id).ToHashSet();
            var amount7 = orders.Where(x => x.Amount == 7).Select(x => x.Id).ToHashSet();

            using (var session = store.OpenSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue; // paging through every match is many queries

                // count-only queries that take the match's count: no where clause, a string term and a numeric term
                Assert.Equal(orders.Count, session.Query<Order, Orders_ByCompany>().Count());
                Assert.Equal(companyA.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A"));
                Assert.Equal(amount7.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Amount == 7));

                // composite shapes whose count is only an estimate, so they still read every entry
                Assert.Equal(companyAAndAmount7.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A" && x.Amount == 7));
                Assert.Equal(companyAOrC.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company == "A" || x.Company == "C"));
                Assert.Equal(orders.Count - companyA.Count, session.Query<Order, Orders_ByCompany>().Count(x => x.Company != "A"));
                Assert.Equal(orders.Count(x => x.Amount > 7), session.Query<Order, Orders_ByCompany>().Count(x => x.Amount > 7));

                // a single token on a full text search field
                Assert.Equal(orders.Count, session.Query<Order, Orders_ByCompany>().Search(x => x.Name, "order").Count());

                // a page without an order by still reports the total of every match
                AssertPage(session.Query<Order, Orders_ByCompany>().Take(10), all, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(5).Take(10), companyA, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count - 3).Take(10), companyA, 3);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").Skip(companyA.Count + 5).Take(10), companyA, 0);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Amount == 7).Take(10), amount7, 10);
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A" && x.Amount == 7).Take(10), companyAAndAmount7, Math.Min(10, companyAAndAmount7.Count));

                // a sorted page takes the sorting path, which counts on its own
                AssertPage(session.Query<Order, Orders_ByCompany>().Where(x => x.Company == "A").OrderBy(x => x.Name).Take(10), companyA, 10);

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

                // paging through every match without an order by visits each document exactly once
                AssertPagingVisitsEveryDocumentOnce(session, x => x.Company == "A", companyA, 7);
                AssertPagingVisitsEveryDocumentOnce(session, null, all, 97);
            }
        }

        private static void AssertFanoutCountsAndPages(IDocumentStore store, List<Order> orders)
        {
            var entries = orders.SelectMany(o => o.Lines.Select(l => (o.Id, l.Product))).ToList();
            var all = orders.Where(x => x.Lines.Count > 0).Select(x => x.Id).ToHashSet();

            using (var session = store.OpenSession())
            {
                // no where clause: the index's entry count, compared with an OR of every product, which has to read
                var products = new[] { "p0", "p1", "p2", "p3" };
                Assert.Equal(entries.Count, session.Query<Order, Orders_ByLineProduct>().Count());
                Assert.Equal(entries.Count, session.Query<Order, Orders_ByLineProduct>()
                    .Count(x => x.Lines.Any(l => l.Product == "p0" || l.Product == "p1" || l.Product == "p2" || l.Product == "p3")));

                // a page is cut by document, while the total counts entries
                AssertPage(session.Query<Order, Orders_ByLineProduct>().Take(10), all, 10, entries.Count);

                foreach (var product in products.Append("no-such-product"))
                {
                    var documents = entries.Where(e => e.Product == product).Select(e => e.Id).ToHashSet();
                    var entriesWithProduct = entries.Count(e => e.Product == product);

                    Assert.Equal(entriesWithProduct, session.Query<Order, Orders_ByLineProduct>().Count(x => x.Lines.Any(l => l.Product == product)));
                    AssertPage(session.Query<Order, Orders_ByLineProduct>().Where(x => x.Lines.Any(l => l.Product == product)).Take(10), documents, Math.Min(10, documents.Count), entriesWithProduct);
                }
            }
        }

        private List<Order> SeedOrdersWithRepeatedProducts(IDocumentStore store)
        {
            // up to four lines, two by two of the same product, and no lines at all on every fourth order
            var orders = Enumerable.Range(0, 2000).Select(i => new Order
            {
                Id = $"orders/{i}",
                Company = "B",
                Name = $"order {i:D4}",
                Lines = Enumerable.Range(0, i % 4).Select(j => new OrderLine { Product = $"p{(i + j / 2) % 3}" }).ToList()
            }).ToList();

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

        private static void AssertMultiValuedCountsAndPages(IDocumentStore store, List<Order> orders)
        {
            using (var session = store.OpenSession())
            {
                Assert.Equal(orders.Count, session.Query<Order, Orders_ByProducts>().Count());

                foreach (var product in new[] { "p0", "p1", "p2", "no-such-product" })
                {
                    var withProduct = orders.Where(x => x.Lines.Any(l => l.Product == product)).Select(x => x.Id).ToHashSet();
                    var query = $"from index 'Orders/ByProducts' where Products = '{product}'";

                    Assert.Equal(withProduct.Count, session.Advanced.RawQuery<Order>(query).Count());

                    var results = session.Advanced.RawQuery<Order>(query).Statistics(out var stats).Take(10).ToList();
                    Assert.Equal(withProduct.Count, stats.TotalResults);
                    Assert.Equal(Math.Min(10, withProduct.Count), results.Count);
                    Assert.All(results, x => Assert.Contains(x.Id, withProduct));
                }
            }
        }

        private static void AssertMultiMapCountsAndPages(IDocumentStore store, List<Order> orders, List<Invoice> invoices)
        {
            var companies = orders.Select(x => (x.Id, x.Company)).Concat(invoices.Select(x => (x.Id, x.Company))).ToList();

            using (var session = store.OpenSession())
            {
                Assert.Equal(companies.Count, session.Query<OrdersAndInvoices_ByCompany.Entry, OrdersAndInvoices_ByCompany>().Count());

                foreach (var company in new[] { "A", "B", "C", "D", "no-such-company" })
                {
                    var expected = companies.Where(x => x.Company == company).Select(x => x.Id).ToHashSet();
                    Assert.Equal(expected.Count, session.Query<OrdersAndInvoices_ByCompany.Entry, OrdersAndInvoices_ByCompany>().Count(x => x.Company == company));

                    var page = session.Query<OrdersAndInvoices_ByCompany.Entry, OrdersAndInvoices_ByCompany>()
                        .Statistics(out var stats)
                        .Where(x => x.Company == company)
                        .Take(10)
                        .ToList();

                    Assert.Equal(expected.Count, stats.TotalResults);
                    Assert.Equal(Math.Min(10, expected.Count), page.Count);
                }
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
                    Amount = i % 50,
                    Lines = Enumerable.Range(0, i % 3 + 1).Select(j => new OrderLine { Product = $"p{j}" }).ToList()
                });
            }

            // several batches with indexing in between, so the index is written by more than one batch
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
            public long Amount { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
        }

        private class Invoice
        {
            public string Id { get; set; }
            public string Company { get; set; }
        }

        private class Orders_ByCompany : AbstractIndexCreationTask<Order>
        {
            public Orders_ByCompany()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    o.Company,
                                    o.Name,
                                    o.Amount
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

        private class Orders_ByProducts : AbstractIndexCreationTask<Order>
        {
            public Orders_ByProducts()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    Products = o.Lines.Select(l => l.Product)
                                };
            }
        }

        private class OrdersAndInvoices_ByCompany : AbstractMultiMapIndexCreationTask<OrdersAndInvoices_ByCompany.Entry>
        {
            public class Entry
            {
                public string Company { get; set; }
            }

            public OrdersAndInvoices_ByCompany()
            {
                AddMap<Order>(orders => from o in orders select new Entry { Company = o.Company });
                AddMap<Invoice>(invoices => from i in invoices select new Entry { Company = i.Company });
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
