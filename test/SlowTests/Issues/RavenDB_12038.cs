using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12038 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Order, Index1.Result>
        {
            public class Result
            {
                public int Year { get; set; }

                public int Month { get; set; }

                public DateTime Date { get; set; }

                public int Count { get; set; }
            }

            public Index1()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Month = 10,
                                    Year = 2018,
                                    Date = DateTime.MinValue,
                                    Count = 1
                                };

                Reduce = results => from result in results
                                    group result by new { result.Month, result.Year, result.Date } into g
                                    select new
                                    {
                                        g.Key.Year,
                                        g.Key.Month,
                                        Date = new DateTime(g.Key.Year, g.Key.Month, 1),
                                        Count = g.Sum(x => x.Count),
                                    };
            }
        }

        [Fact]
        public void CanUseDateTimeConstructorInIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order());

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Index1.Result, Index1>()
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(10, results[0].Month);
                    Assert.Equal(2018, results[0].Year);
                }
            }
        }
    }
}
