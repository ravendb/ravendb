using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11815 : RavenTestBase
    {
        private class MapReduceIndexWithNestedField : AbstractIndexCreationTask<Order, MapReduceIndexWithNestedField.Result>
        {
            public class Result
            {
                public Total Total { get; set; }

                public int Count { get; set; }
            }

            public MapReduceIndexWithNestedField()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Total = new
                                    {
                                        Amount = order.Amount,
                                        Currency = order.Currency
                                    },
                                    Count = 1,
                                };

                Reduce = results => from result in results
                                    group result by new
                                    {
                                        result.Total.Currency
                                    } into g
                                    select new
                                    {
                                        Total = new
                                        {
                                            Amount = g.Sum(x => x.Total.Amount),
                                            Currency = g.Key.Currency
                                        },
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class MapReduceIndexWithNestedField2 : AbstractIndexCreationTask<Order, MapReduceIndexWithNestedField.Result>
        {
            public class Result
            {
                public Total Total { get; set; }

                public int Count { get; set; }
            }

            public MapReduceIndexWithNestedField2()
            {
                Map = orders => from order in orders
                    select new
                    {
                        Total = new
                        {
                            Amount = order.Amount,
                            Currency = order.Currency
                        },
                        Count = 1,
                    };

                Reduce = results => from result in results
                    group result by result.Total.Currency into g
                    select new
                    {
                        Total = new
                        {
                            Amount = g.Sum(x => x.Total.Amount),
                            Currency = g.Key
                        },
                        Count = g.Sum(x => x.Count)
                    };
            }
        }

        private class Order
        {
            public string Id { get; set; }

            public decimal Amount { get; set; }

            public string Currency { get; set; }
        }

        private class Total
        {
            public decimal Amount { get; set; }

            public string Currency { get; set; }
        }

        [Fact]
        public void CanUseNestedFieldValueInGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduceIndexWithNestedField().Execute(store);
                new MapReduceIndexWithNestedField2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Amount = 11,
                        Currency = "USD"
                    });

                    session.Store(new Order
                    {
                        Amount = 50,
                        Currency = "PLN"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results1 = session.Query<MapReduceIndexWithNestedField.Result, MapReduceIndexWithNestedField>()
                        .ToList();

                    Assert.Equal(2, results1.Count);
                    Assert.True(results1.Any(x => x.Total.Currency == "PLN"));
                    Assert.True(results1.Any(x => x.Total.Currency == "USD"));

                    var results2 = session.Query<MapReduceIndexWithNestedField2.Result, MapReduceIndexWithNestedField2>()
                        .ToList();

                    Assert.Equal(2, results2.Count);
                    Assert.True(results2.Any(x => x.Total.Currency == "PLN"));
                    Assert.True(results2.Any(x => x.Total.Currency == "USD"));
                }
            }
        }
    }
}
