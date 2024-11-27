using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_128 : RavenTestBase
    {
        public RDBC_128(ITestOutputHelper output) : base(output)
        {
        }

        public class Invoice
        {
            public string Symbol { get; set; }
            public int Amount { get; set; }
            public decimal Price { get; set; }
        }

        public class Stock
        {
            public string Id { get; set; }

            public string Symbol { get => Id; set => Id = value; }

            public string Name { get; set; }

            public int Age { get; set; }
        }


        public class Invoices_Search : AbstractIndexCreationTask<Invoice, Invoices_Search.Result>
        {
            public class Result
            {
                public decimal Total;
                public string Name;
                public string Symbol;
            }

            public Invoices_Search()
            {
                Map = invoices =>
                    from invoice in invoices
                    let stock = LoadDocument<Stock>(invoice.Symbol)
                    select new
                    {
                        Total = invoice.Amount * invoice.Price * stock.Age,
                        stock.Name,
                        invoice.Symbol
                    };
                Reduce = results =>
                    from result in results
                    group result by result.Symbol
                    into g
                    select new
                    {
                        Name = g.FirstOrDefault().Name,
                        Total = g.Sum(x => x.Total),
                        Symbol = g.Key
                    };
            }
        }


        [Fact]
        public void IndexingOfLoadDocumentWhileChanged()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 500; i++)
                    {
                        bulk.Store(new Stock
                        {
                            Age = 0,
                            Name = "Long name #" + i,
                            Symbol = "SY" + i
                        });
                    }

                    for (int i = 0; i < 5_000; i++)
                    {
                        bulk.Store(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 500)
                        });
                    }

                    new Invoices_Search().Execute(store);

                    for (int i = 0; i < 5_000; i++)
                    {
                        bulk.Store(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 500)
                        });
                    }
                }

                var op = store.Operations.Send(new PatchByQueryOperation(@"
from Stocks
update {
    this.Age++;
}
"));
                op.WaitForCompletion(TimeSpan.FromMinutes(5));

                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(500, s.Count);
                    foreach (var item in s)
                    {
                        Assert.Equal(240, item.Total);
                    }
                }
            }
        }

        [Fact]
        public async Task IndexingOfLoadDocument_UnderLowMemory()
        {
            using (var store = GetDocumentStore())
            {
                const int numberOfSocks = 100;

                Assert.True(numberOfSocks > Raven.Server.Documents.Indexes.Index.MinMapReduceBatchSize,
                    "this test used to fail once number of references per document is greater than min indexing batch size of " +
                    "map-reduce index that is forced under low memory");

                Invoices_Search invoicesSearch = new Invoices_Search();
                await invoicesSearch.ExecuteAsync(store);

                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        await bulk.StoreAsync(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 100)
                        });
                    }
                }

                Indexes.WaitForIndexing(store);

                await store.Maintenance.SendAsync(new StopIndexingOperation());

                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await bulk.StoreAsync(new Stock
                        {
                            Age = 1,
                            Name = "Long name #" + i,
                            Symbol = "SY" + i
                        });
                    }
                }

                (await GetDatabase(store.Database)).IndexStore.GetIndex(invoicesSearch.IndexName).SimulateLowMemory();

                await store.Maintenance.SendAsync(new StartIndexingOperation());

                using (var session = store.OpenAsyncSession())
                {
                    var s = await session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults(waitTimeout: TimeSpan.FromMinutes(3)))
                        .ToListAsync();

                    Assert.Equal(100, s.Count);

                    foreach (var item in s)
                    {
                        Assert.Equal(1200, item.Total);
                    }
                }
            }
        }

        [Fact]
        public async Task IndexingOfLoadDocumentWhileChanged_UnderLowMemory()
        {
            using (var store = GetDocumentStore())
            {
                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 500; i++)
                    {
                        await bulk.StoreAsync(new Stock
                        {
                            Age = 1,
                            Name = "Long name #" + i,
                            Symbol = "SY" + i
                        });
                    }

                    for (int i = 0; i < 5_000; i++)
                    {
                        await bulk.StoreAsync(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 500)
                        });
                    }

                    Invoices_Search invoicesSearch = new Invoices_Search();
                    await invoicesSearch.ExecuteAsync(store);

                    (await GetDatabase(store.Database)).IndexStore.GetIndex(invoicesSearch.IndexName).SimulateLowMemory();

                    for (int i = 0; i < 5_000; i++)
                    {
                        await bulk.StoreAsync(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 500)
                        });
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var s = await session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults(waitTimeout: TimeSpan.FromMinutes(3)))
                        .ToListAsync();

                    Assert.Equal(500, s.Count);
                    foreach (var item in s)
                    {
                        Assert.Equal(240, item.Total);
                    }
                }

                var op = await store.Operations.SendAsync(new PatchByQueryOperation(@"
from Stocks
update {
    this.Age++;
}
"));
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = store.OpenAsyncSession())
                {
                    var s = await session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToListAsync();

                    Assert.Equal(500, s.Count);

                    foreach (var item in s)
                    {
                        Assert.Equal(480, item.Total);
                    }
                }
            }
        }
    }
}
