using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic
{
    public class DynamicMapReduceTests : RavenTestBase
    {
        [Fact]
        public async Task Group_by_string_calculate_count()
        {
            using (var store = await GetDocumentStore())
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
                    var addressesCount =
                        session.Query<Address>().Customize(x => x.WaitForNonStaleResults()).GroupBy(x => x.City).Select(
                            x =>
                                new
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
            }
        }

        [Fact]
        public async Task Group_by_string_calculate_sum()
        {
            using (var store = await GetDocumentStore())
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
            }
        }

        [Fact]
        public async Task Group_by_does_not_support_custom_equality_comparer()
        {
            using (var store = await GetDocumentStore())
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