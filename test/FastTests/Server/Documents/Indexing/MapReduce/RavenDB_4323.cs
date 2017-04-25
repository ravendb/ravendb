using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_4323 : RavenTestBase
    {
        [Fact]
        public async Task ReduceResultsBackAsDocuments()
        {
            var date = new DateTime(2017, 1, 1);
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice {Amount = 1, IssuedAt = date.AddHours(i * 6)});
                    }
                    date = date.AddYears(1);
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12) });
                        await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18) });
                    }
                    await session.SaveChangesAsync();
                }

                await store.ExecuteIndexAsync(new DailyInvoicesIndex());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                await store.ExecuteIndexAsync(new YearlyInvoicesIndex());

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(120, await session.Query<Invoice>().CountAsync());
                    Assert.Equal(93, await session.Query<DailyInvoice>().CountAsync());
                    Assert.Equal(31, await session.Query<MonthlyInvoice>().CountAsync());
                    Assert.Equal(4, await session.Query<YearlyInvoice>().CountAsync());
                }
            }
        }

        [Fact]
        public async Task ShouldNotAllowOutputReduceDocumentsOnTheDocumentsWeMap()
        {
            using (var store = GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new MonthlySelfReduceIndex()));
                Assert.Contains("The collection name (DailyInvoices) cannot be used as this index (MonthlySelfReduceIndex) is mapping this collection and this will result in an infinite loop.", exception.Message);
            }
        }

        [Fact]
        public async Task ShouldNotAllowOutputReduceDocumentsInAInfiniteLoop()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndex());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new YearlyToDailyInfiniteLoopIndex()));
                Assert.Contains($"DailyInvoicesIndex: Invoices => DailyInvoices{Environment.NewLine}MonthlyInvoicesIndex: DailyInvoices => MonthlyInvoices{Environment.NewLine}--> YearlyToDailyInfiniteLoopIndex: MonthlyInvoices => *Invoices*", exception.Message);
            }
        }

        public class Marker
        {
            public string Name { get; set; }
        }

        public class Invoice
        {
            public string Id { get; set; }
            public decimal Amount { get; set; }
            public DateTime IssuedAt { get; set; }
        }

        public class DailyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        public class MonthlyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        public class YearlyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
        }

        public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
        {
            public DailyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new DailyInvoice
                    {
                        Date = invoice.IssuedAt.Date,
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new DailyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "DailyInvoices";
            }
        }

        public class MonthlyInvoicesIndex : AbstractIndexCreationTask<DailyInvoice, MonthlyInvoice>
        {
            public MonthlyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new MonthlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, invoice.Date.Month, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new MonthlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "MonthlyInvoices";
            }
        }

        public class YearlyInvoicesIndex : AbstractIndexCreationTask<MonthlyInvoice, YearlyInvoice>
        {
            public YearlyInvoicesIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new YearlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, 1, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new YearlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "YearlyInvoices";
            }
        }

        public class MonthlySelfReduceIndex : AbstractIndexCreationTask<DailyInvoice, DailyInvoice>
        {
            public MonthlySelfReduceIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new MonthlyInvoice
                    {
                        Date = new DateTime(invoice.Date.Year, invoice.Date.Month, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new MonthlyInvoice
                    {
                        Date = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "DailyInvoices";
            }
        }

        public class YearlyToDailyInfiniteLoopIndex : AbstractIndexCreationTask<MonthlyInvoice, YearlyInvoice>
        {
            public YearlyToDailyInfiniteLoopIndex()
            {
                Map = invoices =>
                    from invoice in invoices
                    select new Invoice
                    {
                        IssuedAt = new DateTime(invoice.Date.Year, 1, 1),
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Date
                    into g
                    select new Invoice
                    {
                        IssuedAt = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "Invoices";
            }
        }
    }

    public class RavenDB_4323_Replication : ReplicationTestsBase, IDocumentTombstoneAware
    {
        [Fact]
        public async Task ReduceOutputShouldNotBeReplicated()
        {
            var date = new DateTime(2017, 2, 14);
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);
                await store1.ExecuteIndexAsync(new RavenDB_4323.DailyInvoicesIndex());

                using (var session = store1.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new RavenDB_4323.Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
                    }
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store1);

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new RavenDB_4323.Marker { Name = "Marker" }, "marker");
                    await session.SaveChangesAsync();
                }
                Assert.True(WaitForDocument(store2, "marker"));

                var collectionStatistics = await store1.Admin.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(8, collectionStatistics.Collections["DailyInvoices"]);
                Assert.Equal(30, collectionStatistics.Collections["Invoices"]);

                collectionStatistics = await store2.Admin.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(30, collectionStatistics.Collections["Invoices"]);
                Assert.False(collectionStatistics.Collections.ContainsKey("DailyInvoices"));
                Assert.Equal(32, collectionStatistics.CountOfDocuments);

                // Check that we do not replicate tombstones of aritifical documents
                var database = await GetDocumentDatabaseInstanceFor(store1);
                var database2 = await GetDocumentDatabaseInstanceFor(store2);
                database.DocumentTombstoneCleaner.Subscribe(this);
                database2.DocumentTombstoneCleaner.Subscribe(this);

                var operation = await store1.Operations.SendAsync(new DeleteCollectionOperation("Invoices"));
                await operation.WaitForCompletionAsync();
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new RavenDB_4323.Marker { Name = "Marker 2" }, "marker2");
                    await session.SaveChangesAsync();
                }
                WaitForIndexing(store1);
                Assert.True(WaitForDocument(store2, "marker2"));

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                using (context.OpenReadTransaction())
                {
                    var dailyInvoicesTombstones = database.DocumentsStorage.GetTombstonesFrom(context, "DailyInvoices", 0, 0, 128).Count();
                    Assert.Equal(8, dailyInvoicesTombstones);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database2))
                using (var tx = context.OpenReadTransaction())
                {
                    var dailyInvoicesTombstones = database.DocumentsStorage.GetTombstonesFrom(context, "DailyInvoices", 0, 0, 128).Count();
                    Assert.Equal(0, dailyInvoicesTombstones);
                    var collections = database.DocumentsStorage.GetTombstoneCollections(tx.InnerTransaction).ToList();
                    Assert.Equal(4, collections.Count);
                    Assert.DoesNotContain("DailyInvoices", collections, StringComparer.OrdinalIgnoreCase);
                }
            }
        }

        protected override void ModifyReplicationDestination(ReplicationNode replicationNode)
        {
            replicationNode.SkipIndexReplication = true;
        }

        public Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            return new Dictionary<string, long>
            {
                ["DailyInvoices"] = 0
            };
        }
    }
}