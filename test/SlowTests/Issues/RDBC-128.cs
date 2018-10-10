using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow.Platform;

namespace SlowTests.Issues
{
    public class RDBC_128 : RavenTestBase
    {
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
                //public Record[] Records;
            }

            //public class Record
            //{
            //    public string Id;
            //    public long Etag;
            //    public int Age;
            //}

        public Invoices_Search()
            {
                Map = invoices =>
                    from invoice in invoices
                    let stock = LoadDocument<Stock>(invoice.Symbol)
                    //let mt = MetadataFor(stock)
                    select new
                    {
                        Total = invoice.Amount * invoice.Price * stock.Age,
                        stock.Name,
                        invoice.Symbol//,
                        //Records = new [] { new {Id = stock.Id, Etag = mt["@etag"], Age = stock.Age } }
                    };
                Reduce = results =>
                    from result in results
                    group result by result.Symbol
                    into g
                    select new
                    {
                        Name = g.FirstOrDefault().Name,
                        Total = g.Sum(x => x.Total),
                        Symbol = g.Key//,
                        //Records = g.SelectMany(x=>x.Records)
                        //    .GroupBy(x=>x.Id)
                        //    .Select(a => a.OrderByDescending(z=>z.Etag).First())
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
                        //if (i % 500 == 0)
                        //{
                        //    continue;
                        //}
                    }
                }
               // WaitForIndexing(store);
                var op = store.Operations.Send(new PatchByQueryOperation(@"
from Stocks
update {
    this.Age++;
}
"));
                op.WaitForCompletion();

                //if (PlatformDetails.Is32Bits)
                //{
                //    Thread.Sleep(500);
                //}

                // todo: 'check if there is indexes cache on 32bit
                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(500, s.Count);
                    foreach (var item in s)
                    {
                        try
                        {
                            Assert.Equal(240, item.Total);
                            //Console.WriteLine("item.Total    "+ item.Total);
                        }
                        catch (Exception)
                        {
                            WaitForUserToContinueTheTest(store);
                        }
                        //Assert.Equal(240, item.Total);
                    }
                }
            }
        }

        [Fact]
        public void IndexingOfLoadDocument_UnderLowMemory()
        {
            using (var store = GetDocumentStore())
            {
                const int numberOfSocks = 100;

                Assert.True(numberOfSocks > Raven.Server.Documents.Indexes.Index.MinMapReduceBatchSize,
                    "this test used to fail once number of references per document is greater than min indexing batch size of " +
                    "map-reduce index that is forced under low memory");

                Invoices_Search invoicesSearch = new Invoices_Search();
                invoicesSearch.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        bulk.Store(new Invoice
                        {
                            Amount = 4,
                            Price = 3,
                            Symbol = "SY" + (i % 100)
                        });
                    }
                }

                WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulk.Store(new Stock
                        {
                            Age = 1,
                            Name = "Long name #" + i,
                            Symbol = "SY" + i
                        });
                    }
                }
                
                GetDatabase(store.Database).Result.IndexStore.GetIndex(invoicesSearch.IndexName).SimulateLowMemory();

                store.Maintenance.Send(new StartIndexingOperation());
                
                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults(waitTimeout: TimeSpan.FromMinutes(3)))
                        .ToList();

                    Assert.Equal(100, s.Count);

                    foreach (var item in s)
                    {
                        Assert.Equal(1200, item.Total);
                    }
                }
            }
        }

        [Fact]
        public void IndexingOfLoadDocumentWhileChanged_UnderLowMemory()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 500; i++)
                    {
                        bulk.Store(new Stock
                        {
                            Age = 1,
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

                    Invoices_Search invoicesSearch = new Invoices_Search();
                    invoicesSearch.Execute(store);

                    GetDatabase(store.Database).Result.IndexStore.GetIndex(invoicesSearch.IndexName).SimulateLowMemory();

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

                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults(waitTimeout: TimeSpan.FromMinutes(3)))
                        .ToList();
                    Assert.Equal(500, s.Count);
                    foreach (var item in s)
                    {
                        Assert.Equal(240, item.Total);
                    }
                }

                var op = store.Operations.Send(new PatchByQueryOperation(@"
from Stocks
update {
    this.Age++;
}
"));
                op.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

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
