using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14926 : RavenTestBase
    {
        public RavenDB_14926(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateIndexWithEnumerableRangeInSelectMany()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new AccruedRevenueIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new Invoice
                    {
                        CreatedOn = DateTime.Now,
                        SubTotal = 345
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<AccruedRevenueIndex.Result, AccruedRevenueIndex>()
                        .ToList();

                    Assert.Equal(12, results.Count);
                }
            }
        }

        // Document type for Invoice
        private class Invoice
        {
            public DateTime CreatedOn { get; set; }
            public decimal SubTotal { get; set; }
        }

        // Divides the revenue from invoices over the 12-month period they are for.
        private class AccruedRevenueIndex : AbstractIndexCreationTask<Invoice, AccruedRevenueIndex.Result>
        {
            public AccruedRevenueIndex()
            {
                Map = items =>
                    from inv in items
                    let portion = Math.Round(inv.SubTotal / 12, 2)
                    let error = inv.SubTotal - 12 * portion
                    let firstDate = new DateTime(inv.CreatedOn.Year, inv.CreatedOn.Month, 1)
                    from month in Enumerable.Range(0, 12)
                    select new
                    {
                        YearMonth = firstDate.AddMonths(month).ToString("yyyy-MM"),
                        AccruedRevenue = portion + (month == 0 ? error : 0)
                    };

                Reduce = results =>
                    from r in results
                    group r by r.YearMonth
                    into g
                    select new
                    {
                        YearMonth = g.Key,
                        AccruedRevenue = g.Sum(x => x.AccruedRevenue)
                    };
            }

            public class Result
            {
                public string YearMonth { get; set; }
                public decimal AccruedRevenue { get; set; }
            }
        }
    }
}
