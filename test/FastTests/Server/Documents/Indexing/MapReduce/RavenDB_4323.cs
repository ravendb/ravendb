using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Operations.Databases;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;
using Xunit.Sdk;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_4323 : RavenNewTestBase
    {
        [Fact]
        public async Task ReductResultsBackAsDocuments()
        {
            var date = DateTime.Today;
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

                store.ExecuteIndex(new DailyInvoicesIndex());
                store.ExecuteIndex(new MonthlyInvoicesIndex());
                store.ExecuteIndex(new YearlyInvoicesIndex());
                //TODO: Use await store.ExecuteIndexAsync(new DailyInvoices());
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
            var date = DateTime.Today;
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        await session.StoreAsync(new DailyInvoice { Amount = 1, Date = date.AddHours(i * 6) });
                    }
                    await session.SaveChangesAsync();
                }

                store.ExecuteIndex(new MonthlySelfReduceIndex());
                //TODO: Use await store.ExecuteIndexAsync(new DailyInvoices());

                DatabaseStatistics statistics;
                while (true)
                {
                    statistics = await store.Admin.ForDatabase(store.DefaultDatabase).SendAsync(new GetStatisticsOperation());
                    var index = statistics.Indexes
                        .FirstOrDefault(x => x.Name == nameof(MonthlySelfReduceIndex));
                    if (index.State == IndexState.Error)
                    {
                        var errors = await store.Admin.ForDatabase(store.DefaultDatabase).SendAsync(new GetIndexErrorsOperation(new [] {nameof(MonthlySelfReduceIndex)}));
                        Assert.Equal("Invalid", errors.Single().Errors.Single().Action);
                        return;
                    }
                    if (index.IsStale == false)
                        break;
                }

                Assert.True(false, "Index should be marked as invalid");
            }
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

                OutputReduceResultsToCollectionName = "DailyInvoices";
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

                OutputReduceResultsToCollectionName = "MonthlyInvoices";
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

                OutputReduceResultsToCollectionName = "YearlyInvoices";
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

                OutputReduceResultsToCollectionName = "DailyInvoices";
            }
        }
    }
}