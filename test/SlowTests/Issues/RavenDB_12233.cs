using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12233 : RavenTestBase
    {
        public RavenDB_12233(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task FailingTest_Context()
        {
            const int size = 7000;
            var s = new string((char)0, size);

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var djv = new DynamicJsonValue
                {
                    ["ProductName"] = s
                };

                var json = context.ReadObject(djv, "input");

                AssertItem(json, string.Join(string.Empty, Enumerable.Range(0, size).Select(x => "\\u0000")));

                await using (var ms = new MemoryStream())
                {
                    await json.WriteJsonToAsync(ms);
                    ms.Position = 0;

                    var inputJson = json.ToString();
                    using (var sr = new StreamReader(ms, Encoding.UTF8, true, 4096, true))
                        Assert.Equal(inputJson, await sr.ReadToEndAsync());

                    ms.Position = 0;
                    json = await context.ReadForMemoryAsync(ms, "json");
                    AssertItem(json, s);
                }
            }

            void AssertItem(BlittableJsonReaderObject result, string expected)
            {
                Assert.True(result.TryGet("ProductName", out string productName));

                if (productName != expected)
                    throw new InvalidOperationException($"Expected: {s} but was {productName}.");
            }
        }

        [Fact]
        public void FailingTest_BulkInsert()
        {
            const int count = 500;
            var s = new string((char)0, 1);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new Order
                        {
                            Company = $"companies/{i}",
                            Employee = $"employee/{i}",
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    Product = $"products/{i}",
                                    ProductName = s
                                },
                                new OrderLine
                                {
                                    Product = $"products/{i}",
                                    ProductName = s
                                },
                            }
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<Order>("orders/");
                    while (enumerator.MoveNext())
                    {
                        var order = enumerator.Current.Document;
                        foreach (var line in order.Lines)
                        {
                            Assert.Equal(s, line.ProductName);
                        }
                    }
                }
            }
        }

        [Fact]
        public void FailingTest_MapReduce()
        {
            using (var store = GetDocumentStore())
            {
                const int count = 300;

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        bulk.Store(new Order()
                        {
                            Company = $"companies/{i}",
                            Employee = $"employee/{i}",
                            Lines = new List<OrderLine>()
                            {
                                new OrderLine()
                                {
                                    Product = $"products/{i}",
                                    ProductName = new string((char)0, 1) + "/" + i
                                },
                                new OrderLine()
                                {
                                    Product = $"products/{i}",
                                    ProductName = new string((char)0, 1) + "/" + i
                                },
                            }
                        });
                    }
                }

                var index = new SimpleIndex();

                index.Execute(store);

                for (int i = 0; i < 10; i++)
                {
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(count + 1, stats.CountOfDocuments); // + hilo
                    var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());
                    Assert.Equal(1 + 1, collectionStats.Collections.Count); // + hilo
                    Assert.Equal(count, collectionStats.Collections["Orders"]);

                    using (var session = store.OpenSession())
                    {
                        var c = session.Query<Order, SimpleIndex>().Customize(x =>
                        {
                            x.NoCaching();
                            x.WaitForNonStaleResults(TimeSpan.FromMinutes(2));
                        }).Count();

                        Assert.Equal(count, c);
                    }
                }
            }
        }

        private class SimpleIndex : AbstractIndexCreationTask<Order, SimpleIndex.Result>
        {
            public class Result
            {
                public string ProductName { get; set; }

                public int Count { get; set; }
            }

            public SimpleIndex()
            {
                Map = orders => from order in orders
                                from item in order.Lines
                                select new
                                {
                                    ProductName = item.ProductName,
                                    Count = 1
                                };

                Reduce = results => from result in results
                                    group result by result.ProductName into g
                                    select new
                                    {
                                        ProductName = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}
