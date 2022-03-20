using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1411 : RavenTestBase
    {
        public RavenDB_1411(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanUpdateMapReduceIndexWithLoadDocumentAfterDocumentDeletion()
        {
            using (var store = GetDocumentStore())
            {
                string companyId2;
                using (var session = store.OpenAsyncSession())
                {
                    var company1 = new Company
                    {
                        Name = "Hibernating Rhinos"
                    };
                    await session.StoreAsync(company1);
                    await session.StoreAsync(new Order
                    {
                        CompanyId = company1.Id
                    });

                    var company2 = new Company
                    {
                        Name = "Hibernating Rhinos"
                    };
                    await session.StoreAsync(company2);
                    companyId2 = company2.Id;

                    await session.StoreAsync(new Order
                    {
                        CompanyId = companyId2
                    });

                    await session.SaveChangesAsync();
                }

                new OrdersMapReduceIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<OrdersMapReduceIndex.Result, OrdersMapReduceIndex>().ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal("Hibernating Rhinos", list[0].CompanyName);
                    Assert.Equal(2, list[0].Count);
                }

                store.Maintenance.Send(new StopIndexOperation(nameof(OrdersMapReduceIndex)));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(companyId2);
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);
                await database.TombstoneCleaner.ExecuteCleanup();

                store.Maintenance.Send(new StartIndexOperation(nameof(OrdersMapReduceIndex)));
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<OrdersMapReduceIndex.Result, OrdersMapReduceIndex>().ToList();
                    Assert.Equal(2, list.Count);
                    Assert.Equal("Hibernating Rhinos", list[0].CompanyName);
                    Assert.Equal(1, list[0].Count);
                    Assert.Equal("HR", list[1].CompanyName);
                    Assert.Equal(1, list[1].Count);
                }
            }
        }

        private class Order
        {
            public string Id { get; set; }

            public string CompanyId { get; set; }
        }

        private class Company
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class OrdersMapReduceIndex : AbstractIndexCreationTask<Order, OrdersMapReduceIndex.Result>
        {
            public class Result
            {
                public string CompanyName { get; set; }

                public int Count { get; set; }
            }

            public OrdersMapReduceIndex()
            {
                Map = orders => from f in orders
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(f.CompanyId).Name ?? "HR",
                        Count = 1
                    };

                Reduce =
                    results =>
                        from result in results
                        group result by result.CompanyName
                        into g
                        select new Result
                        {
                            CompanyName = g.Key,
                            Count = g.Sum(x => x.Count)
                        };
            }
        }
    }
}
