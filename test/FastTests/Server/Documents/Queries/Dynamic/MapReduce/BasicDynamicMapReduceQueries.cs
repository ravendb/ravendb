using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Static;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Dynamic.MapReduce
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class BasicDynamicMapReduceQueries : RavenTestBase
    {
        public BasicDynamicMapReduceQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Group_by_string_calculate_count()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Hadera"
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var addressesCount = session.Query<Address>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.City)
                        .Select(x => new
                        {
                            City = x.Key,
                            Count = x.Count(),
                        })
                        .Where(x => x.Count == 2)
                        .ToList();

                    Assert.Equal(2, addressesCount[0].Count);
                    Assert.Equal("Torun", addressesCount[0].City);

                    var addressesTotalCount =
                        session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City).Select(
                            x =>
                                new AddressReduceResult // using class instead of anonymous object
                                {
                                    City = x.Key,
                                    TotalCount = x.Count(),
                                })
                            .Where(x => x.TotalCount == 2)
                            .ToList();

                    Assert.Equal(2, addressesTotalCount[0].TotalCount);
                    Assert.Equal("Torun", addressesTotalCount[0].City);
                }

                // using different syntax
                using (var session = store.OpenSession())
                {
                    var addressesCount =
                        session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City, x => 1,
                            (key, g) => new
                            {
                                City = key,
                                Count = g.Count()
                            }).Where(x => x.Count == 2)
                            .ToList();

                    Assert.Equal(2, addressesCount[0].Count);
                    Assert.Equal("Torun", addressesCount[0].City);

                    var addressesTotalCount =
                        session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City, x => 1,
                            (key, g) => new AddressReduceResult // using class instead of anonymous object
                            {
                                City = key,
                                TotalCount = g.Count()
                            }).Where(x => x.TotalCount == 2)
                            .ToList();

                    Assert.Equal(2, addressesTotalCount[0].TotalCount);
                    Assert.Equal("Torun", addressesTotalCount[0].City);
                }

                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Equal(1, indexDefinitions.Length); // all of the above queries should be handled by the same auto index
                Assert.Equal("Auto/Addresses/ByCountReducedByCity", indexDefinitions[0].Name);
            }
        }

        [Fact]
        public async Task Group_by_string_calculate_sum()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Chair",
                        Quantity = 1
                    });
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Chair",
                        Quantity = 3
                    });
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Desk",
                        Quantity = 2
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var sumOfLinesByName =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new
                                {
                                    NameOfProduct = x.Key,
                                    TotalQuantity = x.Sum(_ => _.Quantity)
                                })
                            .ToList();

                    Assert.Equal(2, sumOfLinesByName.Count);

                    Assert.Equal(4, sumOfLinesByName[0].TotalQuantity);
                    Assert.Equal("Chair", sumOfLinesByName[0].NameOfProduct);

                    Assert.Equal(2, sumOfLinesByName[1].TotalQuantity);
                    Assert.Equal("Desk", sumOfLinesByName[1].NameOfProduct);

                    var sumOfLinesByNameClass =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new OrderLineReduceResult
                                {
                                    NameOfProduct = x.Key,
                                    OrderedQuantity = x.Sum(_ => _.Quantity)
                                })
                            .ToList();

                    Assert.Equal(2, sumOfLinesByNameClass.Count);

                    Assert.Equal(4, sumOfLinesByNameClass[0].OrderedQuantity);
                    Assert.Equal("Chair", sumOfLinesByNameClass[0].NameOfProduct);

                    Assert.Equal(2, sumOfLinesByNameClass[1].OrderedQuantity);
                    Assert.Equal("Desk", sumOfLinesByNameClass[1].NameOfProduct);
                }

                // different GroupBy syntax
                using (var session = store.OpenSession())
                {
                    var sumOfLinesByNameClass =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName, x => x.Quantity,
                            (anyKeyName, g) =>
                                new OrderLineReduceResult
                                {
                                    NameOfProduct = anyKeyName,
                                    OrderedQuantity = g.Sum()
                                })
                            .Where(x => x.NameOfProduct == "Chair")
                            .ToList();

                    Assert.Equal(1, sumOfLinesByNameClass.Count);

                    Assert.Equal(4, sumOfLinesByNameClass[0].OrderedQuantity);
                    Assert.Equal("Chair", sumOfLinesByNameClass[0].NameOfProduct);

                    var sumOfLinesByName =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName, x => x.Quantity,
                            (anyKeyName, g) =>
                                new
                                {
                                    NameOfProduct = anyKeyName,
                                    OrderedQuantity = g.Sum()
                                })
                            .Where(x => x.OrderedQuantity == 2)
                            .ToList();

                    Assert.Equal(1, sumOfLinesByName.Count);

                    Assert.Equal(2, sumOfLinesByName[0].OrderedQuantity);
                    Assert.Equal("Desk", sumOfLinesByName[0].NameOfProduct);
                }

                // different GroupBy syntax
                using (var session = store.OpenSession())
                {
                    var sumOfLinesByName =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName, x => x.Quantity)
                            .Select((group, i) => new
                            {
                                Name = group.Key,
                                OrderedQuantity = group.Sum(x => x)
                            })
                            .Where(x => x.Name == "Chair")
                            .ToList();

                    Assert.Equal(1, sumOfLinesByName.Count);

                    Assert.Equal(4, sumOfLinesByName[0].OrderedQuantity);
                    Assert.Equal("Chair", sumOfLinesByName[0].Name);

                    var sumOfLinesByNameClass =
                        session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName, x => x.Quantity)
                            .Select((group, i) => new OrderLineReduceResult
                            {
                                NameOfProduct = group.Key,
                                OrderedQuantity = group.Sum(x => x)
                            })
                            .Where(x => x.OrderedQuantity == 2)
                            .ToList();

                    Assert.Equal(1, sumOfLinesByNameClass.Count);

                    Assert.Equal(2, sumOfLinesByNameClass[0].OrderedQuantity);
                    Assert.Equal("Desk", sumOfLinesByNameClass[0].NameOfProduct);
                }

                var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Equal(1, indexDefinitions.Length); // all of the above queries should be handled by the same auto index
                Assert.Equal("Auto/OrderLines/ByQuantityReducedByProductName", indexDefinitions[0].Name);
            }
        }

        [Fact]
        public void Group_by_does_not_support_custom_equality_comparer()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                    {
                        session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City, x => 1,
                            (key, g) => new
                            {
                                City = key,
                                Count = g.Count()
                            }, StringComparer.OrdinalIgnoreCase)
                            .ToList();
                    });
                }
            }
        }

        [Fact]
        public async Task Can_project_in_map_reduce()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Torun"
                    });
                    await session.StoreAsync(new Address()
                    {
                        City = "Hadera"
                    });

                    await session.SaveChangesAsync();
                }

                using (var commands = store.Commands())
                {
                    // create auto map reduce index
                    var command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Addresses GROUP BY City SELECT count() as TotalCount ",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    // retrieve only City field
                    command = new QueryCommand(commands.Session, new IndexQuery
                    {
                        Query = "FROM Addresses GROUP BY City SELECT City ",
                        WaitForNonStaleResults = true
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    var indexDefinitions = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                    Assert.Equal(1, indexDefinitions.Length); // the above queries should be handled by the same auto index

                    var result = command.Result;
                    var results = new DynamicArray(result.Results);

                    var cities = new List<string> { "Torun", "Hadera" };
                    Assert.Equal(2, results.Count());
                    foreach (dynamic r in results)
                    {
                        var json = (DynamicBlittableJson)r;
                        Assert.Equal(2, json.BlittableJson.Count);
                        Assert.True(json.ContainsKey("City"));
                        Assert.True(json.ContainsKey(Constants.Documents.Metadata.Key));

                        var city = r.City;
                        Assert.True(cities.Remove(city));
                    }
                }
            }
        }

        [Fact]
        public async Task Order_by_string_integer_and_decimal_fields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Chair",
                        Quantity = 1,
                        PricePerUnit = 1.2m
                    });
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Chair",
                        Quantity = 3,
                        PricePerUnit = 3.3m
                    });
                    await session.StoreAsync(new OrderLine
                    {
                        ProductName = "Desk",
                        Quantity = 2,
                        PricePerUnit = 2.7m
                    });

                    await session.SaveChangesAsync();
                }

                // order by string
                using (var session = store.OpenSession())
                {
                    var items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new OrderLineReduceResult
                                {
                                    NameOfProduct = x.Key,
                                    OrderedQuantity = x.Sum(_ => _.Quantity)
                                })
                            .OrderBy(x => x.NameOfProduct)
                            .ToList();

                    Assert.Equal("Chair", items[0].NameOfProduct);
                    Assert.Equal("Desk", items[1].NameOfProduct);

                    items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new OrderLineReduceResult
                                {
                                    NameOfProduct = x.Key,
                                    OrderedQuantity = x.Sum(_ => _.Quantity)
                                })
                            .OrderByDescending(x => x.NameOfProduct)
                            .ToList();

                    Assert.Equal("Desk", items[0].NameOfProduct);
                    Assert.Equal("Chair", items[1].NameOfProduct);
                }

                // order by int
                using (var session = store.OpenSession())
                {
                    var items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new
                                {
                                    NameOfProduct = x.Key,
                                    TotalQuantity = x.Sum(_ => _.Quantity)
                                })
                            .OrderBy(x => x.TotalQuantity)
                            .ToList();

                    Assert.Equal("Desk", items[0].NameOfProduct);
                    Assert.Equal("Chair", items[1].NameOfProduct);

                    items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new
                                {
                                    NameOfProduct = x.Key,
                                    TotalQuantity = x.Sum(_ => _.Quantity)
                                })
                            .OrderByDescending(x => x.TotalQuantity)
                            .ToList();

                    Assert.Equal("Chair", items[0].NameOfProduct);
                    Assert.Equal("Desk", items[1].NameOfProduct);
                }

                // order by decimal
                using (var session = store.OpenSession())
                {
                    var items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new
                                {
                                    NameOfProduct = x.Key,
                                    TotalPricePerUnit = x.Sum(_ => _.PricePerUnit)
                                })
                            .OrderBy(x => x.TotalPricePerUnit)
                            .ToList();

                    Assert.Equal("Desk", items[0].NameOfProduct);
                    Assert.Equal("Chair", items[1].NameOfProduct);

                    items = session.Query<OrderLine>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.ProductName).Select(
                            x =>
                                new
                                {
                                    NameOfProduct = x.Key,
                                    TotalPricePerUnit = x.Sum(_ => _.PricePerUnit)
                                })
                            .OrderByDescending(x => x.TotalPricePerUnit)
                            .ToList();

                    Assert.Equal("Chair", items[0].NameOfProduct);
                    Assert.Equal("Desk", items[1].NameOfProduct);
                }
            }
        }

        [Fact]
        public void Group_by_nested_field_sum_on_collection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        ShipTo = new Address { Country = "Norway" },
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 2
                            },
                            new OrderLine
                            {
                                Quantity = 2
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        ShipTo = new Address { Country = "Norway" },
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 1
                            }
                        }
                    });

                    session.Store(new Order
                    {
                        ShipTo = new Address { Country = "Sweden" },
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                Quantity = 1
                            }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => x.ShipTo.Country)
                            .Select(x => new
                            {
                                Country = x.Key,
                                OrderedQuantity = x.Sum(order => order.Lines.Sum(line => line.Quantity))
                            })
                            .OrderBy(x => x.Country)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal("Norway", orders[0].Country);
                    Assert.Equal(5, orders[0].OrderedQuantity);

                    Assert.Equal("Sweden", orders[1].Country);
                    Assert.Equal(1, orders[1].OrderedQuantity);
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .GroupBy(x => x.ShipTo.Country, x => x.Lines.Sum(line => line.Quantity))
                            .Select((group, i) => new
                            {
                                Country = group.Key,
                                OrderedQuantity = group.Sum(x => x)
                            })
                            .OrderBy(x => x.Country)
                            .ToList();

                    Assert.Equal(2, orders.Count);

                    Assert.Equal("Norway", orders[0].Country);
                    Assert.Equal(5, orders[0].OrderedQuantity);

                    Assert.Equal("Sweden", orders[1].Country);
                    Assert.Equal(1, orders[1].OrderedQuantity);
                }
            }
        }

        [Fact]
        public void Group_by_number()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 30
                    });

                    session.Store(new User
                    {
                        Age = 30
                    });

                    session.SaveChanges();

                    var results = session.Query<User>().GroupBy(x => x.Age).Select(x => new
                    {
                        Age = x.Key,
                        Count = x.Count(),
                    }).ToList();

                    Assert.Equal(2, results[0].Count);
                }
            }
        }

        public class AddressReduceResult
        {
            public string City { get; set; }
            public int TotalCount { get; set; }
        }

        public class OrderLineReduceResult
        {
            public string NameOfProduct { get; set; }
            public int OrderedQuantity { get; set; }
        }
    }
}
