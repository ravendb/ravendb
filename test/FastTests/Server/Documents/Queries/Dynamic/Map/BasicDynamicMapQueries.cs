using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure.Entities;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class BasicDynamicMapQueries : RavenTestBase
    {
        public BasicDynamicMapQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task String_where_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Arek", users[0].Name);
                }
            }
        }

        [Fact]
        public async Task Numeric_where_equals_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Foo", Age = 40 });
                    await session.StoreAsync(new User { Name = "Bar", Age = 50 });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age == 50).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Bar", users[0].Name);
                }
            }
        }

        [Fact]
        public async Task Numeric_between_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Foo", Age = 40 });
                    await session.StoreAsync(new User { Name = "Bar", Age = 50 });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age >= 50 && x.Age <= 60).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Bar", users[0].Name);


                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age >= 30.1 && x.Age <= 40.0).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Foo", users[0].Name);
                }
            }
        }

        [Fact]
        public async Task Numeric_range_where_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak", Age = 40 });
                    await session.StoreAsync(new User { Name = "Arek", Age = 50 });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age > 40).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Arek", users[0].Name);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age < 50).ToList();
                    Assert.Equal(1, users.Count);
                    Assert.Equal("Fitzchak", users[0].Name);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age >= 40).ToList();
                    Assert.Equal(2, users.Count);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age <= 50).ToList();
                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public async Task Where_clause_and_sorting()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Arek", Age = 25 }, "users/1");
                    await session.StoreAsync(new User { Name = "Jan", Age = 27 }, "users/2");
                    await session.StoreAsync(new User { Name = "Arek", Age = 39 }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var usersSortedByAge = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").OrderBy(x => x.Age).ToList();

                    Assert.Equal(2, usersSortedByAge.Count);
                    Assert.Equal("users/1", usersSortedByAge[0].Id);
                    Assert.Equal("users/3", usersSortedByAge[1].Id);
                }

                using (var session = store.OpenSession())
                {
                    var usersSortedByAge = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "Arek").OrderByDescending(x => x.Age).ToList();

                    Assert.Equal(2, usersSortedByAge.Count);
                    Assert.Equal("users/3", usersSortedByAge[0].Id);
                    Assert.Equal("users/1", usersSortedByAge[1].Id);
                }
            }
        }

        [Fact]
        public async Task Sorting_by_doubles()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Camera { Megapixels = 1.3 }, "cameras/1");
                    await session.StoreAsync(new Camera { Megapixels = 0.5 }, "cameras/2");
                    await session.StoreAsync(new Camera { Megapixels = 1.0 }, "cameras/3");
                    await session.StoreAsync(new Camera { Megapixels = 2.0 }, "cameras/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var cameras = session.Query<Camera>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Megapixels).ToList();

                    Assert.Equal("cameras/2", cameras[0].Id);
                    Assert.Equal("cameras/3", cameras[1].Id);
                    Assert.Equal("cameras/1", cameras[2].Id);
                    Assert.Equal("cameras/4", cameras[3].Id);
                }
            }
        }

        [Fact]
        public async Task Sorting_by_integers()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Camera { Zoom = 5 }, "cameras/1");
                    await session.StoreAsync(new Camera { Zoom = 10 }, "cameras/2");
                    await session.StoreAsync(new Camera { Zoom = 40 }, "cameras/3");
                    await session.StoreAsync(new Camera { Zoom = 15 }, "cameras/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var cameras = session.Query<Camera>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Zoom).ToList();

                    Assert.Equal("cameras/1", cameras[0].Id);
                    Assert.Equal("cameras/2", cameras[1].Id);
                    Assert.Equal("cameras/4", cameras[2].Id);
                    Assert.Equal("cameras/3", cameras[3].Id);
                }
            }
        }

        [Fact]
        public async Task Sorting_by_nested_string_field()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { ShipTo = new Address { Country = "Poland" } }, "orders/1");
                    await session.StoreAsync(new Order { ShipTo = new Address { Country = "Israel" } }, "orders/2");
                    await session.StoreAsync(new Order { ShipTo = new Address { Country = "USA" } }, "orders/3");
                    await session.StoreAsync(new Order { ShipTo = new Address { Country = "Canada" } }, "orders/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.ShipTo.Country).ToList();

                    Assert.Equal("orders/4", orders[0].Id);
                    Assert.Equal("orders/2", orders[1].Id);
                    Assert.Equal("orders/1", orders[2].Id);
                    Assert.Equal("orders/3", orders[3].Id);

                    orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).OrderByDescending(x => x.ShipTo.Country).ToList();

                    Assert.Equal("orders/3", orders[0].Id);
                    Assert.Equal("orders/1", orders[1].Id);
                    Assert.Equal("orders/2", orders[2].Id);
                    Assert.Equal("orders/4", orders[3].Id);

                    var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10)).ToList();

                    Assert.Equal(1, indexes.Count);
                    Assert.Equal("Auto/Orders/ByShipTo.Country", indexes[0].Name);
                }
            }
        }

        [Fact]
        public async Task Sorting_by_nested_integer_field()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { ShipTo = new Address { ZipCode = 3000 } }, "orders/1");
                    await session.StoreAsync(new Order { ShipTo = new Address { ZipCode = 222 } }, "orders/2");
                    await session.StoreAsync(new Order { ShipTo = new Address { ZipCode = 5599 } }, "orders/3");
                    await session.StoreAsync(new Order { ShipTo = new Address { ZipCode = 111 } }, "orders/4");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.ShipTo.ZipCode).ToList();

                    Assert.Equal("orders/4", orders[0].Id);
                    Assert.Equal("orders/2", orders[1].Id);
                    Assert.Equal("orders/1", orders[2].Id);
                    Assert.Equal("orders/3", orders[3].Id);

                    orders = session.Query<Order>().Customize(x => x.WaitForNonStaleResults()).OrderByDescending(x => x.ShipTo.ZipCode).ToList();

                    Assert.Equal("orders/3", orders[0].Id);
                    Assert.Equal("orders/1", orders[1].Id);
                    Assert.Equal("orders/2", orders[2].Id);
                    Assert.Equal("orders/4", orders[3].Id);

                    var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10)).ToList();

                    Assert.Equal(1, indexes.Count);
                    Assert.Equal("Auto/Orders/ByShipTo.ZipCode", indexes[0].Name);
                }
            }
        }

        [Fact]
        public async Task Sorting_by_strings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "David" }, "users/1");
                    await session.StoreAsync(new User { Name = "Adam" }, "users/2");
                    await session.StoreAsync(new User { Name = "John" }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal("users/2", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/3", users[2].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderByDescending(x => x.Name).ToList();

                    Assert.Equal("users/3", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/2", users[2].Id);
                }
            }
        }

        [Fact]
        public async Task Partial_match()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "David", Age = 31 }, "users/1");
                    await session.StoreAsync(new User { Name = "Adam", Age = 12 }, "users/2");
                    await session.StoreAsync(new User { Name = "John", Age = 24 }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal("users/2", users[0].Id);
                    Assert.Equal("users/1", users[1].Id);
                    Assert.Equal("users/3", users[2].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Age > 20).ToList();

                    Assert.Equal(2, users.Count);
                    Assert.Equal("users/1", users[0].Id);
                    Assert.Equal("users/3", users[1].Id);

                    var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10))
                        .OrderBy(x=>x.Name.Length)
                        .ToList();

                    Assert.Equal("Auto/Users/ByName", indexes[0].Name);
                    Assert.Equal("Auto/Users/ByAgeAndName", indexes[1].Name);
                }
            }
        }

        [Fact]
        public async Task Empty_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "David", Age = 31 }, "users/1");
                    await session.StoreAsync(new User { Name = "Adam", Age = 12 }, "users/2");
                    await session.StoreAsync(new User { Name = "John", Age = 24 }, "users/3");
                    await session.StoreAsync(new User { Name = "James", Age = 24 }, "users/4");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(4, users.Count);
                    Assert.Equal("users/1", users[0].Id);
                    Assert.Equal("users/2", users[1].Id);
                    Assert.Equal("users/3", users[2].Id);
                    Assert.Equal("users/4", users[3].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Skip(1).Take(2).ToList();

                    Assert.Equal(2, users.Count);
                    Assert.Equal("users/2", users[0].Id);
                    Assert.Equal("users/3", users[1].Id);

                    users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Skip(3).Take(10).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("users/4", users[0].Id);
                }
            }
        }

        [Fact]
        public async Task Can_project_in_map()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak", Age = 30 });
                    await session.StoreAsync(new User { Name = "Arek", Age = 31 });
                    await session.StoreAsync(new Order { ShipTo = new Address { City = "New York", Country = "USA" }, Employee = "Arek" });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var names = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Age > 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(2, names.Count);
                    Assert.True(names.Any(x => x == "Arek"));
                    Assert.True(names.Any(x => x == "Fitzchak"));

                    var complex = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Age > 10)
                        .Select(x => new
                        {
                            x.Name,
                            x.Age
                        })
                        .ToList();

                    Assert.Equal(2, complex.Count);
                    Assert.True(complex.Any(x => x.Name == "Arek" && x.Age == 31));
                    Assert.True(complex.Any(x => x.Name == "Fitzchak" && x.Age == 30));

                    var nested = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Employee == "Arek")
                        .Select(x => new
                        {
                            x.Employee,
                            x.ShipTo.Country,
                            x.ShipTo.City
                        })
                        .ToList();

                    Assert.Equal(1, nested.Count);
                    Assert.Equal("New York", nested[0].City);
                    Assert.Equal("USA", nested[0].Country);
                }
            }
        }

        [Fact]
        public void Collection_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.Store(new User { Name = "Arek" }, "users/2");
                    session.Store(new User { Name = "Joe" }, "users/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == "users/2").ToList();

                    Assert.Equal(1, users.Count);
                    Assert.Equal("Arek", users[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id.In("users/1", "users/3")).ToList();

                    Assert.Equal(2, users.Count);

                    Assert.True(users.Any(x => x.Name == "Fitzchak"));
                    Assert.True(users.Any(x => x.Name == "Joe"));
                }
            }
        }

        [Fact]
        public void Can_query_on_dictionary()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new DictItem
                    {
                        NumericDict = new Dictionary<int, int>
                        {
                            {0, 2},
                            {1, 2}
                        },
                        StringDict = new Dictionary<string, string>()
                        {
                            { "a", "b" },
                            { "b", "b" },
                            { "c c", "b" }
                        }
                    });

                    session.Store(new DictItem
                    {
                        NumericDict = new Dictionary<int, int>(),
                        StringDict = new Dictionary<string, string>()
                        {
                            { "a", "c" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringDict["a"] == "b").ToList();
                    Assert.Equal(1, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringDict.Any(y => y.Key == "a")).ToList();
                    Assert.Equal(2, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringDict.Any(y => y.Value == "b" && y.Key == "b")).ToList();
                    Assert.Equal(1, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringDict.Any(y => y.Value == "c" || y.Key == "b")).ToList();
                    Assert.Equal(2, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.NumericDict[1] == 2).ToList();
                    Assert.Equal(1, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.NumericDict[1] >= 2).ToList();
                    Assert.Equal(1, items.Count);

                    items = session.Query<DictItem>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringDict["c c"] == "b").ToList(); // where StringDict.'c c' = 'b'
                    Assert.Equal(1, items.Count);
                }
            }
        }

        private class DictItem
        {
            public Dictionary<int, int> NumericDict { get; set; }
            public Dictionary<string, string> StringDict { get; set; }
        }
    }
}
