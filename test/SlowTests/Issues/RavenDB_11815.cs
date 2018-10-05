using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11815 : RavenTestBase
    {
        private class MapReduceIndexWithNestedField : AbstractIndexCreationTask<Order, Result>
        {
            public MapReduceIndexWithNestedField()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Total = new
                                    {
                                        order.Amount,
                                        order.Currency
                                    },
                                    Count = 1
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
                                            g.Key.Currency
                                        },
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class MultiMapReduceIndexWithNestedField : AbstractMultiMapIndexCreationTask<Result>
        {
            public MultiMapReduceIndexWithNestedField()
            {
                AddMap<Order>(orders => from order in orders
                                        select new
                                        {
                                            Total = new
                                            {
                                                order.Amount,
                                                order.Currency
                                            },
                                            Count = 1
                                        });

                AddMap<Order2>(orders => from order in orders
                                         select new
                                         {
                                             Total = new
                                             {
                                                 order.Currency,
                                                 order.Amount
                                             },
                                             Count = 1
                                         });

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
                                            g.Key.Currency
                                        },
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class MapReduceIndexWithNestedField2 : AbstractIndexCreationTask<Order, Result>
        {
            public MapReduceIndexWithNestedField2()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Total = new
                                    {
                                        order.Amount,
                                        order.Currency
                                    },
                                    Count = 1
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

        private class Result
        {
            public Total Total { get; set; }

            public int Count { get; set; }
        }

        private class Order
        {
            public decimal Amount { get; set; }

            public string Currency { get; set; }
        }

        private class Order2 : Order
        {
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
                //new MapReduceIndexWithNestedField().Execute(store);
                //new MapReduceIndexWithNestedField2().Execute(store);
                new MultiMapReduceIndexWithNestedField().Execute(store);

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

                    //

                    session.Store(new Order2
                    {
                        Amount = 11,
                        Currency = "USD"
                    });

                    session.Store(new Order2
                    {
                        Amount = 2,
                        Currency = "USD"
                    });

                    session.Store(new Order2
                    {
                        Amount = 50,
                        Currency = "PLN"
                    });

                    session.Store(new Order2
                    {
                        Amount = 3,
                        Currency = "PLN"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    //AssertResults<MapReduceIndexWithNestedField>(session);

                    //AssertResults<MapReduceIndexWithNestedField2>(session);

                    //AssertResults<MapReduceIndexWithNestedFieldJs>(session);

                    //AssertResults<MapReduceIndexWithNestedFieldJs2>(session);

                    AssertResults<MultiMapReduceIndexWithNestedField>(session);
                }
            }

            void AssertResults<TIndex>(IDocumentSession session) where TIndex : AbstractIndexCreationTask, new()
            {
                var results1 = session.Query<Result, TIndex>()
                    .ToList();

                Assert.Equal(2, results1.Count);
                Assert.True(results1.Any(x => x.Total.Currency == "PLN"));
                Assert.True(results1.Any(x => x.Total.Currency == "USD"));
            }
        }
    }
}
