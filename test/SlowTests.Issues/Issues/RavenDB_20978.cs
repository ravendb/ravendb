using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20978 : RavenTestBase
{
    public RavenDB_20978(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Run_Facet_Query_On_Map_Reduce_Index(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            await new Orders_ByCompany().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new Order
                    {
                        Company = $"companies/{i}",
                        Lines = new List<OrderLine>
                        {
                            new()
                            {
                                Discount = 0.1m,
                                PricePerUnit = 10,
                                Quantity = i
                            }
                        }
                    });
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session.Query<Orders_ByCompany.Result, Orders_ByCompany>()
                    .AggregateBy(x => x.ByField(a => a.Company))
                    .AndAggregateBy(x => x.ByRanges(a => a.Total < 200, a => a.Total >= 200 && a.Total <= 400))
                    .ExecuteAsync();

                var companyResults = results["Company"];
                Assert.Equal(10, companyResults.Values.Count);

                var totalResults = results["Total"];
                Assert.Equal(2, totalResults.Values.Count);

                var range1Results = totalResults.Values[0];
                Assert.Equal(10, range1Results.Count);

                var range2Results = totalResults.Values[1];
                Assert.Equal(0, range2Results.Count);
            }
        }
    }

    private class Orders_ByCompany : AbstractIndexCreationTask<Order, Orders_ByCompany.Result>
    {
        public class Result
        {
            public string Company { get; set; }
            public int Count { get; set; }
            public decimal Total { get; set; }
        }

        public Orders_ByCompany()
        {
            Map = orders => from order in orders
                            select new Result
                            {
                                Company = order.Company,
                                Count = 1,
                                Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
                            };

            Reduce = results => from result in results
                                group result by result.Company into g
                                select new
                                {
                                    Company = g.Key,
                                    Count = g.Sum(x => x.Count),
                                    Total = g.Sum(x => x.Total)
                                };
        }
    }
}
