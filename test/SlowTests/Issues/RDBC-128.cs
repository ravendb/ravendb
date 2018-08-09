using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Raven.Client.Documents.Operations;

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
                op.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var s = session.Query<Invoices_Search.Result, Invoices_Search>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(500, s.Count);
                    foreach (var item in s)
                    {
                        Assert.Equal(240, item.Total);
                    }
                }
            }
        }
    }
}
