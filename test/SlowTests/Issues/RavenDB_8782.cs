using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8782 : RavenTestBase
    {
        public RavenDB_8782(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_group_by_composite_key_with_custom_names()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Orders_ByProductAndCount_MethodSyntax());
                store.ExecuteIndex(new Orders_ByProductAndCount_QuerySyntax());

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                           new OrderLine
                           {
                               Product = "products/1",
                               PricePerUnit = 10,
                               Quantity = 1
                           },
                            new OrderLine
                            {
                                Product = "products/2",
                                PricePerUnit = 20,
                                Quantity = 1
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Product = "products/1",
                                PricePerUnit = 10,
                                Quantity = 1
                            }
                        }
                    });

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    var results = session.Query<Orders_ByProductAndCount_MethodSyntax.Result, Orders_ByProductAndCount_MethodSyntax>()
                        .OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal("products/2", results[0].Product);
                    Assert.Equal(1, results[0].Count);
                    Assert.Equal(20, results[0].Total);

                    Assert.Equal("products/1", results[1].Product);
                    Assert.Equal(2, results[1].Count);
                    Assert.Equal(20, results[1].Total);

                    results = session.Query<Orders_ByProductAndCount_MethodSyntax.Result, Orders_ByProductAndCount_QuerySyntax>()
                        .OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal("products/2", results[0].Product);
                    Assert.Equal(1, results[0].Count);
                    Assert.Equal(20, results[0].Total);

                    Assert.Equal("products/1", results[1].Product);
                    Assert.Equal(2, results[1].Count);
                    Assert.Equal(20, results[1].Total);
                }
            }
        }

        [Fact]
        public void Should_throw_index_compilation_error_on_attempt_to_group_by_non_existing_field()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "invalidIndex",
                    Maps =
                    {
                        @"from company in docs.Companies
                        select new
                        {
                            City = company.Address.City,
                            Count = 1
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        result.Address
                    }
                    into g
                    select new
                    {
                        City = g.Key.City,
                        Count = g.Sum(x => x.Count)
                    }"
                })));

                Assert.Contains("Group by field 'Address' was not found on the list of index fields (City, Count)", ex.Message);
            }
        }

        private class Orders_ByProductAndCount_MethodSyntax : AbstractIndexCreationTask<Order, Orders_ByProductAndCount_MethodSyntax.Result>
        {
            public class Result
            {
                public string Product { get; set; }

                public int Count { get; set; }

                public int Total { get; set; }
            }

            public Orders_ByProductAndCount_MethodSyntax()
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new
                    {
                        Product = line.Product,
                        Count = 1,
                        Total = ((line.Quantity * line.PricePerUnit) * (1 - line.Discount))
                    };

                Reduce = results => from result in results
                    group result by new
                    {
                        P = result.Product,
                    }
                    into g
                    select new
                    {
                        Product = g.Key.P,
                        Count = g.Sum(x => x.Count),
                        Total = g.Sum(x => x.Total)
                    };
            }
        }

        private class Orders_ByProductAndCount_QuerySyntax : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from order in docs.Orders
                        from line in order.Lines
                        select new
                        {
                            Product = line.Product,
                            Count = 1,
                            Total = ((line.Quantity * line.PricePerUnit) * (1 - line.Discount))
                        }"
                    },
                    Reduce = @"from result in results
                    group result by new
                    {
                        P = result.Product
                    }
                    into g
                    select new
                    {
                        Product = g.Key.P,
                        Count = g.Sum(x => x.Count),
                        Total = g.Sum(x => x.Total)
                    }"
                };
            }
        }
    }
}
