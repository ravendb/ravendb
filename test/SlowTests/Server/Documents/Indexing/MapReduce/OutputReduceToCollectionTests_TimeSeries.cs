using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class OutputReduceToCollectionTests_TimeSeries : RavenTestBase
    {
        public OutputReduceToCollectionTests_TimeSeries(ITestOutputHelper output)
            : base(output)
        {
        }

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
                Assert.Contains($"DailyInvoicesIndex: Invoices (referenced: Invoices) => DailyInvoices{Environment.NewLine}" +
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
                                $"DailyInvoicesIndexLoadDocument: InvoiceHolders (referenced: InvoiceHolders,Invoices) => DailyInvoices{Environment.NewLine}" +
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
                    await session.StoreAsync(new DailyInvoice { Amount = 1, Date = new DateTime(2017, 1, 1) });
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<IndexInvalidException>(async () => await CreateDataAndIndexes(store));
                Assert.Contains("Index 'DailyInvoicesIndex' is defined to output the Reduce results to documents in Collection 'DailyInvoices'." +
                                " This collection currently has 1 document . All documents in Collection 'DailyInvoices' must be deleted first.", exception.Message);
            }
        }

        private async Task CreateDataAndIndexes(IDocumentStore store)
        {
            var date = new DateTime(2017, 1, 1);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 30; i++)
                {
                    var invoice = new Invoice { Amount = 1, IssuedAt = date.AddHours(i * 6) };
                    await session.StoreAsync(invoice);

                    session.TimeSeriesFor(invoice, "Views").Append(invoice.IssuedAt, 6);
                }
                date = date.AddYears(1);
                for (int i = 0; i < 30; i++)
                {
                    var invoice1 = new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 6) };
                    await session.StoreAsync(invoice1);
                    session.TimeSeriesFor(invoice1, "Views").Append(invoice1.IssuedAt, 6);

                    var invoice2 = new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 12) };
                    await session.StoreAsync(invoice2);
                    session.TimeSeriesFor(invoice2, "Views").Append(invoice2.IssuedAt, 12);

                    var invoice3 = new Invoice { Amount = 1, IssuedAt = date.AddMonths(i).AddHours(i * 18) };
                    await session.StoreAsync(invoice3);
                    session.TimeSeriesFor(invoice3, "Views").Append(invoice3.IssuedAt, 18);
                }
                await session.SaveChangesAsync();
            }

            await store.ExecuteIndexAsync(new DailyInvoicesIndex());
            await store.ExecuteIndexAsync(new MonthlyInvoicesIndex());
            await store.ExecuteIndexAsync(new YearlyInvoicesIndex());

            WaitForIndexing(store);

            Assert.True(WaitForValue(() =>
            {
                var stats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                return stats.Collections != null && stats.Collections.Count >= 5;
            }, true));

            RavenTestHelper.AssertNoIndexErrors(store);
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
            public decimal Count { get; set; }
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

        private class DailyInvoicesIndex : AbstractTimeSeriesIndexCreationTask<Invoice, DailyInvoice>
        {
            public DailyInvoicesIndex()
            {
                AddMap("Views", segments => from segment in segments
                                            let invoice = LoadDocument<Invoice>(segment.DocumentId)
                                            select new DailyInvoice
                                            {
                                                Date = invoice.IssuedAt.Date,
                                                Amount = invoice.Amount
                                            });

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

        public class DailyInvoicesIndexLoadDocument : AbstractTimeSeriesIndexCreationTask<InvoiceHolder, DailyInvoice>
        {
            public DailyInvoicesIndexLoadDocument()
            {
                AddMap("Views", segments => from segment in segments
                                            let invoiceHolder = LoadDocument<InvoiceHolder>(segment.DocumentId)
                                            let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
                                            select new DailyInvoice
                                            {
                                                Date = invoice.IssuedAt.Date,
                                                Amount = invoice.Amount
                                            });

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

        private class MonthlySelfReduceIndex : AbstractTimeSeriesIndexCreationTask<DailyInvoice, DailyInvoice>
        {
            public MonthlySelfReduceIndex()
            {
                AddMap("Views", segments => from segment in segments
                                            let invoice = LoadDocument<DailyInvoice>(segment.DocumentId)
                                            select new MonthlyInvoice
                                            {
                                                Date = new DateTime(invoice.Date.Year, invoice.Date.Month, 1),
                                                Amount = invoice.Amount
                                            });
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

        private class InvoiceSelfReduceLoadDocumentIndex : AbstractTimeSeriesIndexCreationTask<InvoiceHolder, Invoice>
        {
            public InvoiceSelfReduceLoadDocumentIndex()
            {
                AddMap("Views", segments => from segment in segments
                                            let invoiceHolder = LoadDocument<InvoiceHolder>(segment.DocumentId)
                                            let invoice = LoadDocument<Invoice>(invoiceHolder.InvoiceId)
                                            select new Invoice
                                            {
                                                IssuedAt = invoice.IssuedAt,
                                                Amount = invoice.Amount
                                            });

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

        private class YearlyToDailyInfiniteLoopIndex : AbstractTimeSeriesIndexCreationTask<MonthlyInvoice, Invoice>
        {
            public YearlyToDailyInfiniteLoopIndex()
            {
                AddMap("Views", segments => from segment in segments
                                            let invoice = LoadDocument<MonthlyInvoice>(segment.DocumentId)
                                            select new Invoice
                                            {
                                                IssuedAt = new DateTime(invoice.Date.Year, 1, 1),
                                                Amount = invoice.Amount
                                            });

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
    }
}
