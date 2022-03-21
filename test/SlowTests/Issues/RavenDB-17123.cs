using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17123 : RavenTestBase
    {
        public RavenDB_17123(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateFieldsShouldNotDisplayAsIgnored()
        {
            var createFieldIndex = new CreateFieldIndex();
            var simpleIndex = new SimpleIndex();

            using (var store = GetDocumentStore())
            {
                createFieldIndex.Execute(store);
                simpleIndex.Execute(store);

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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Result, CreateFieldIndex>()
                        .ProjectInto<Result>()
                        .ToList();

                    Assert.Equal(count, query.Count);
                    foreach (var value in query)
                    {
                        Assert.Null(value.Total); // Result.Total is null instead of "__ignored"
                    }

                    query = session.Query<Result, SimpleIndex>()
                        .ProjectInto<Result>()
                        .ToList();

                    long totalExpected = 12;

                    foreach (var value in query)
                    {
                        Assert.NotNull(value.Total);
                        Assert.Equal(totalExpected, value.Total);
                    }
                }
            }
        }

        [Fact]
        public void SpatialCreateFieldsShouldNotDisplayAsIgnored()
        {
            var createSpatialFieldIndex = new CreateSpatialFieldIndex();
            var simpleIndex = new SimpleIndex();

            using (var store = GetDocumentStore())
            {
                createSpatialFieldIndex.Execute(store);
                simpleIndex.Execute(store);

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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Result, CreateSpatialFieldIndex>()
                        .ProjectInto<Result>()
                        .ToList();

                    Assert.Equal(count, query.Count);
                    foreach (var value in query)
                    {
                        Assert.Null(value.Total); // Result.Total is null instead of "__ignored"
                    }

                    query = session.Query<Result, SimpleIndex>()
                        .ProjectInto<Result>()
                        .ToList();

                    long totalExpected = 12;

                    foreach (var value in query)
                    {
                        Assert.NotNull(value.Total);
                        Assert.Equal(totalExpected, value.Total);
                    }
                }
            }
        }

        public class Result
        {
            public string ProductName;
            public object Total;
        }

        private class SimpleIndex : AbstractIndexCreationTask<Order, Result>
        {
            public SimpleIndex()
            {
                Map = orders => from order in orders
                    from item in order.Lines
                    select new
                    {
                        ProductName = item.ProductName,
                        Total = item.Discount
                    };

                Reduce = results => from result in results
                    group result by result.ProductName into g
                    select new
                    {
                        ProductName = g.Key,
                        Total = 12
                    };
            }
        }

        private class CreateSpatialFieldIndex : AbstractIndexCreationTask<Order, Result>
        {
            public CreateSpatialFieldIndex()
            {
                    Map = orders => from order in orders
                                    from item in order.Lines
                                    select new
                                    {
                                        ProductName = item.ProductName,
                                        Total = item.Discount
                                    };

                    Reduce = results => from result in results
                                        group result by result.ProductName into g
                                        select new
                                        {
                                            ProductName = g.Key,
                                            Total = CreateSpatialField(54.2, 23.2)
                                        };
            }
        }

        private class CreateFieldIndex : AbstractIndexCreationTask<Order, Result>
        {
            public CreateFieldIndex()
            {
                Map = orders => from order in orders
                    from item in order.Lines
                    select new
                    {
                        ProductName = item.ProductName,
                        Total = item.Discount
                    };

                Reduce = results => from result in results
                    group result by result.ProductName into g
                    select new
                    {
                        ProductName = g.Key,
                        Total = CreateField("Total", 23.2)
                    };
            }
        }
    }
}
