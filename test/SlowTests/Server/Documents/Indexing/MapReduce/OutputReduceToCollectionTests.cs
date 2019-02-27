using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class OutputReduceToCollectionTests : RavenTestBase
    {
        [Fact]
        public async Task ReduceResultsBackAsDocuments()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                Assert.Equal(120, collectionStats.Collections["Invoices"]);
                Assert.Equal(93, collectionStats.Collections["DailyInvoices"]);
                Assert.Equal(31, collectionStats.Collections["MonthlyInvoices"]);
                Assert.Equal(4, collectionStats.Collections["YearlyInvoices"]);

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
        public async Task ForbidOutputReduceDocumentsOnTheDocumentsWeMap()
        {
            using (var store = GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new MonthlySelfReduceIndex()));
                Assert.Contains("It is forbidden to create the 'MonthlySelfReduceIndex' index " +
                                "which would output reduce results to documents in the 'DailyInvoices' collection, " +
                                "as this index is mapping or referencing the 'DailyInvoices' collection and this will result in an infinite loop.", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsOnTheDocumentsWeLoadInMap()
        {
            using (var store = GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new InvoiceSelfReduceLoadDocumentIndex()));
                Assert.Contains("It is forbidden to create the 'InvoiceSelfReduceLoadDocumentIndex' index " +
                                "which would output reduce results to documents in the 'Invoices' collection, " +
                                "as this index is mapping or referencing the 'Invoices' collection and this will result in an infinite loop.", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsInAInfiniteLoop()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndex());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new YearlyToDailyInfiniteLoopIndex()));
                Assert.Contains($"DailyInvoicesIndex: Invoices => DailyInvoices{Environment.NewLine}" +
                                $"MonthlyInvoicesIndex: DailyInvoices => MonthlyInvoices{Environment.NewLine}" +
                                $"--> YearlyToDailyInfiniteLoopIndex: MonthlyInvoices => *Invoices*", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsInAInfiniteLoopCausedByLoadDocument()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndexLoadDocument());
                await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new YearlyToDailyInfiniteLoopIndex()));
                Assert.Contains($"It is forbidden to create the 'YearlyToDailyInfiniteLoopIndex' index " +
                                "which would output reduce results to documents in the 'Invoices' collection, " +
                                "as 'Invoices' collection is consumed by other index in a way that would lead to an infinite loop." +
                                $"{Environment.NewLine}" +
                                $"DailyInvoicesIndexLoadDocument: InvoiceHolders (referenced: Invoices) => DailyInvoices{Environment.NewLine}" +
                                $"MonthlyInvoicesIndex: DailyInvoices => MonthlyInvoices{Environment.NewLine}" +
                                $"--> YearlyToDailyInfiniteLoopIndex: MonthlyInvoices => *Invoices*", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidOutputReduceDocumentsToExistingCollectionWhichHaveDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new DailyInvoice
                    {
                        Amount = 1,
                        Date = new DateTime(2017, 1, 1)
                    });
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await CreateDataAndIndexes(store));
                Assert.Contains("Index 'DailyInvoicesIndex' is defined to output the Reduce results to documents in Collection 'DailyInvoices'." +
                                " This collection currently has 1 document . All documents in Collection 'DailyInvoices' must be deleted first.", exception.Message);
            }
        }

        [Fact]
        public async Task ForbidSideBySideIndexingWithoutClearingTheOutputReduceToCollectionValueFirst()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new DailyInvoicesIndex());
                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await store.ExecuteIndexAsync(new Replacement.DailyInvoicesIndex()));
                Assert.Contains("In order to create the 'ReplacementOf/DailyInvoicesIndex' side by side index " +
                                "you firstly need to set OutputReduceToCollection to be null on the 'DailyInvoicesIndex' index " +
                                "and than delete all of the documents in the 'DailyInvoices' collection.", exception.Message);
            }
        }

        [Fact]
        public async Task LetTheUserModifyTheIndex()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                await store.ExecuteIndexAsync(new Reset.DailyInvoicesIndex());
                store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM DailyInvoices" })).WaitForCompletion(TimeSpan.FromSeconds(60));
                // We need to wait for the cluster to update the index before overwriting the index again
                WaitForIndexing(store);

                await store.ExecuteIndexAsync(new Replacement.DailyInvoicesIndex());
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
        public async Task CanResetIndex()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDataAndIndexes(store);

                await store.Maintenance.SendAsync(new ResetIndexOperation(new DailyInvoicesIndex().IndexName));
                await store.Maintenance.SendAsync(new ResetIndexOperation(new MonthlyInvoicesIndex().IndexName));
                await store.Maintenance.SendAsync(new ResetIndexOperation(new YearlyInvoicesIndex().IndexName));

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

        private async Task CreateDataAndIndexes(DocumentStore store)
        {
            var date = new DateTime(2017, 1, 1);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 30; i++)
                {
                    await session.StoreAsync(new Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) });
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

            WaitForValue(() =>
            {
                var stats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                return stats.Collections != null && stats.Collections.Count >= 5;
            }, true);

            RavenTestHelper.AssertNoIndexErrors(store);
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

        public class InvoiceHolder
        {
            public string Id { get; set; }
            public string InvoiceId { get; set; }
        }

        public class DailyInvoice
        {
            public DateTime Date { get; set; }
            public decimal Amount { get; set; }
            public decimal Average { get; set; }
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

        public class DailyInvoicesIndexLoadDocument : AbstractIndexCreationTask<InvoiceHolder, DailyInvoice>
        {
            public DailyInvoicesIndexLoadDocument()
            {
                Map = invoiceHolders =>
                    from invoiceHolder in invoiceHolders
                    let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
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

        private class MonthlyInvoicesIndex : AbstractIndexCreationTask<DailyInvoice, MonthlyInvoice>
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

        private class YearlyInvoicesIndex : AbstractIndexCreationTask<MonthlyInvoice, YearlyInvoice>
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

        private class MonthlySelfReduceIndex : AbstractIndexCreationTask<DailyInvoice, DailyInvoice>
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

        private class InvoiceSelfReduceLoadDocumentIndex : AbstractIndexCreationTask<InvoiceHolder, Invoice>
        {
            public InvoiceSelfReduceLoadDocumentIndex()
            {
                Map = invoiceHolders =>
                    from invoiceHolder in invoiceHolders
                    let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
                    select new Invoice
                    {
                        IssuedAt = invoice.IssuedAt,
                        Amount = invoice.Amount
                    };

                Reduce = results =>
                    from r in results
                    group r by r.IssuedAt
                    into g
                    select new Invoice
                    {
                        IssuedAt = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "Invoices";
            }
        }

        private class YearlyToDailyInfiniteLoopIndex : AbstractIndexCreationTask<MonthlyInvoice, Invoice>
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
                    group r by r.IssuedAt
                    into g
                    select new Invoice
                    {
                        IssuedAt = g.Key,
                        Amount = g.Sum(x => x.Amount)
                    };

                OutputReduceToCollection = "Invoices";
            }
        }

        private static class Reset
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                        };

                    OutputReduceToCollection = null;
                }
            }
        }

        private static class Replacement
        {
            public class DailyInvoicesIndex : AbstractIndexCreationTask<Invoice, DailyInvoice>
            {
                public DailyInvoicesIndex()
                {
                    Map = invoices =>
                        from invoice in invoices
                        select new DailyInvoice
                        {
                            Date = invoice.IssuedAt.Date,
                            Amount = invoice.Amount,
                            Average = invoice.Amount,
                        };

                    Reduce = results =>
                        from r in results
                        group r by r.Date
                        into g
                        select new DailyInvoice
                        {
                            Date = g.Key,
                            Amount = g.Sum(x => x.Amount),
                            Average = g.Average(x => x.Amount)
                        };

                    OutputReduceToCollection = "DailyInvoices";
                }
            }
        }
    }
}
