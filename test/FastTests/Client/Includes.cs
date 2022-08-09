using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Includes : RavenTestBase
    {
        public Includes(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Load_With_Include()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var address = new Address { City = "London", Country = "UK" };
                    session.Store(address);
                    session.Store(new User { Name = "Adam", AddressId = session.Advanced.GetDocumentId(address) });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Include<User>(x => x.AddressId).Load<User>("users/1-A");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Include("AddressId").Load<User>("users/1-A");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }
            }
        }

        [Fact]
        public void Can_Load_With_Include_Using_IIncludeBuilder()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var address = new Address { City = "London", Country = "UK" };
                    session.Store(address);
                    session.Store(new User { Name = "Adam", AddressId = session.Advanced.GetDocumentId(address) });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(
                        "users/1-A",
                        i => i.IncludeDocuments(x => x.AddressId));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Include("AddressId").Load<User>("users/1-A");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Use_Includes_Within_Multi_Load(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "users/1", Name = "Daniel Lang" });
                    session.Store(new Customer { Id = "users/2", Name = "Oren Eini" });

                    session.Store(new Order { CustomerId = "users/1", Number = "1" });
                    session.Store(new Order { CustomerId = "users/1", Number = "2" });
                    session.Store(new Order { CustomerId = "users/2", Number = "3" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Include(x => x.CustomerId)
                        .ToList();

                    Assert.Equal(3, orders.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var customers = session.Load<Customer>(orders.Select(x => x.CustomerId));
                    Assert.Equal(2, customers.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Primary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "customers/1" });
                    session.Store(new Order { CustomerId = "customers/1" }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.CustomerId)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer>(order.CustomerId);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Primary_String_Property_Using_IIncludeBuilder()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "customers/1" });
                    session.Store(new Order { CustomerId = "customers/1" }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1234",
                        i => i.IncludeDocuments(x => x.CustomerId));

                    // this will not require querying the server!
                    var cust = session.Load<Customer>(order.CustomerId);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Query_With_Include_By_Primary_String_Property(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Id = "customers/1", Name = "1" });
                    session.Store(new Customer { Id = "customers/2", Name = "2" });
                    session.Store(new Customer { Id = "customers/3", Name = "3" });
                    session.Store(new Order { CustomerId = "customers/1", TotalPrice = 200D }, "orders/1234");
                    session.Store(new Order { CustomerId = "customers/2", TotalPrice = 50D }, "orders/1235");
                    session.Store(new Order { CustomerId = "customers/3", TotalPrice = 300D }, "orders/1236");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Include(x => x.CustomerId)
                        .Where(x => x.TotalPrice > 100)
                        .ToList();

                    Assert.Equal(2, orders.Count);

                    foreach (var order in orders)
                    {
                        // this will not require querying the server!
                        var cust = session.Load<Customer>(order.CustomerId);
                        Assert.NotNull(cust);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Primary_List_Of_Strings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Supplier { Name = "1" });
                    session.Store(new Supplier { Name = "2" });
                    session.Store(new Supplier { Name = "3" });
                    session.Store(new Order { SupplierIds = new[] { "suppliers/1-A", "suppliers/2-A", "suppliers/3-A" } },
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.SupplierIds)
                        .Load("orders/1234");

                    Assert.Equal(3, order.SupplierIds.Count());

                    foreach (var supplierId in order.SupplierIds)
                    {
                        // this will not require querying the server!
                        var supp = session.Load<Supplier>(supplierId);
                        Assert.NotNull(supp);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Include_By_Primary_List_Of_Strings_Using_IIncludeBuilder(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Supplier { Name = "1" });
                    session.Store(new Supplier { Name = "2" });
                    session.Store(new Supplier { Name = "3" });
                    session.Store(new Order { SupplierIds = new[] { "suppliers/1-A", "suppliers/2-A", "suppliers/3-A" } },
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1234",
                        i => i.IncludeDocuments(x => x.SupplierIds));

                    Assert.Equal(3, order.SupplierIds.Count());

                    foreach (var supplierId in order.SupplierIds)
                    {
                        // this will not require querying the server!
                        var supp = session.Load<Supplier>(supplierId);
                        Assert.NotNull(supp);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Secondary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer());
                    session.Store(new Order { Refferal = new Referral { CustomerId = "customers/1-A" } }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.Refferal.CustomerId)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var referrer = session.Load<Customer>(order.Refferal.CustomerId);

                    Assert.NotNull(referrer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Secondary_String_Property_Using_IIncludeBuilder()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer());
                    session.Store(new Order { Refferal = new Referral { CustomerId = "customers/1-A" } }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1234",
                        i => i.IncludeDocuments(x => x.Refferal.CustomerId));

                    // this will not require querying the server!
                    var referrer = session.Load<Customer>(order.Refferal.CustomerId);

                    Assert.NotNull(referrer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_List_Of_Secondary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product { Name = "1" });
                    session.Store(new Product { Name = "2" });
                    session.Store(new Product { Name = "3" });
                    session.Store(
                        new Order
                        {
                            LineItems =
                                new[]
                                {
                                    new LineItem {ProductId = "products/1-A"}, new LineItem {ProductId = "products/2-A"},
                                    new LineItem {ProductId = "products/3-A"}
                                }
                        }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.LineItems.Select(li => li.ProductId))
                        .Load("orders/1234");

                    foreach (var lineItem in order.LineItems)
                    {
                        // this will not require querying the server!
                        var product = session.Load<Product>(lineItem.ProductId);
                        Assert.NotNull(product);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Include_By_List_Of_Secondary_String_Property_Using_IIncludeBuilder(Options options)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product { Name = "1" });
                    session.Store(new Product { Name = "2" });
                    session.Store(new Product { Name = "3" });
                    session.Store(
                        new Order
                        {
                            LineItems =
                                new[]
                                {
                                    new LineItem {ProductId = "products/1-A"}, new LineItem {ProductId = "products/2-A"},
                                    new LineItem {ProductId = "products/3-A"}
                                }
                        }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1234",
                        i => i.IncludeDocuments(x => x
                                .LineItems
                                .Select(li => li.ProductId)));

                    foreach (var lineItem in order.LineItems)
                    {
                        // this will not require querying the server!
                        var product = session.Load<Product>(lineItem.ProductId);
                        Assert.NotNull(product);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_Nested_Dictionary_Values_Property()
        {
            using (var store = GetDocumentStore())
            {
                const string userId1 = "users/1";
                const string userId2 = "users/2";
                string testId;

                using (var session = store.OpenSession())
                {
                    var user1 = new User { Name = "name1", Age = 1 };
                    session.Store(user1, userId1);

                    var user2 = new User { Name = "name2", Age = 1 };
                    session.Store(user2, userId2);

                    var test = new Dictionary
                    {
                        DictionaryByUserId = new Dictionary<string, IdClass>
                        {
                            { userId1, new IdClass {UserId = userId1} },
                            { userId2, new IdClass {UserId = userId2} }
                        }
                    };

                    session.Store(test);
                    testId = test.Id;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<Dictionary, User>(x => x.DictionaryByUserId.Select(d => d.Value.UserId))
                        .Load<Dictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<Dictionary>(x => x.DictionaryByUserId.Values.Select(d => d.UserId))
                        .Load<Dictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_Nested_Dictionary_Keys_Property()
        {
            using (var store = GetDocumentStore())
            {
                const string userId1 = "users/1";
                const string userId2 = "users/2";
                string testId;

                using (var session = store.OpenSession())
                {
                    var user1 = new User { Name = "name1", Age = 1 };
                    session.Store(user1, userId1);

                    var user2 = new User { Name = "name2", Age = 1 };
                    session.Store(user2, userId2);

                    var test = new Dictionary
                    {
                        DictionaryByUserId = new Dictionary<string, IdClass>
                        {
                            { userId1, new IdClass {UserId = userId1} },
                            { userId2, new IdClass {UserId = userId2} }
                        }
                    };

                    session.Store(test);
                    testId = test.Id;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<Dictionary>(x => x.DictionaryByUserId.Keys)
                        .Load<Dictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<Dictionary>(x => x.DictionaryByUserId.Keys.Select(k => k))
                        .Load<Dictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_Dictionary_Key_And_Value_Properties()
        {
            using (var store = GetDocumentStore())
            {
                const string userId1 = "users/1";
                const string userId2 = "users/2";
                string testId;

                using (var session = store.OpenSession())
                {
                    var user1 = new User { Name = "name1", Age = 1 };
                    session.Store(user1, userId1);

                    var user2 = new User { Name = "name2", Age = 1 };
                    session.Store(user2, userId2);

                    var test = new KeyValueDictionary
                    {
                        DictionaryByUserId = new Dictionary<string, string>
                        {
                            { userId1, userId1 },
                            { userId2, userId2 }
                        }
                    };

                    session.Store(test);
                    testId = test.Id;

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<KeyValueDictionary, User>(x => x.DictionaryByUserId.Select(k => k.Key))
                        .Load<KeyValueDictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var test = session
                        .Include<KeyValueDictionary, User>(x => x.DictionaryByUserId.Select(k => k.Value))
                        .Load<KeyValueDictionary>(testId);

                    Assert.NotNull(test);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>(userId1);
                    session.Load<User>(userId2);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        private class Dictionary
        {
            public string Id { get; set; }

            public Dictionary<string, IdClass> DictionaryByUserId { get; set; }
        }

        private class KeyValueDictionary
        {
            public string Id { get; set; }

            public Dictionary<string, string> DictionaryByUserId { get; set; }
        }

        private class IdClass
        {
            public string UserId { get; set; }
        }

        private class Order
        {
            public string Number { get; set; }
            public string CustomerId { get; set; }
            public string[] SupplierIds { get; set; }
            public Referral Refferal { get; set; }
            public LineItem[] LineItems { get; set; }
            public double TotalPrice { get; set; }
        }

        private class Order2
        {
            public int Customer2Id { get; set; }
            public string Customer2IdString { get { return Customer2Id.ToString(CultureInfo.InvariantCulture); } }
            public Guid[] Supplier2Ids { get; set; }
            public Referral2 Refferal2 { get; set; }
            public LineItem2[] LineItem2s { get; set; }
            public double TotalPrice { get; set; }
        }

        private class Order3
        {
            public DenormalizedCustomer Customer { get; set; }
            public string[] SupplierIds { get; set; }
            public Referral Refferal { get; set; }
            public LineItem[] LineItems { get; set; }
            public double TotalPrice { get; set; }
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public short Age { get; set; }
            public string HashedPassword { get; set; }
        }

        private class Customer2
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public short Age { get; set; }
            public string HashedPassword { get; set; }
        }

        private class DenormalizedCustomer
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        private class Supplier
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }

        private class Supplier2
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        private class Referral
        {
            public string CustomerId { get; set; }
            public double CommissionPercentage { get; set; }
        }

        private class Referral2
        {
            public int Customer2Id { get; set; }
            public double CommissionPercentage { get; set; }
        }

        private class LineItem
        {
            public string ProductId { get; set; }
            public string Name { get; set; }
            public int Quantity { get; set; }
            public double Price { get; set; }
        }

        private class LineItem2
        {
            public Guid Product2Id { get; set; }
            public string Name { get; set; }
            public int Quantity { get; set; }
            public double Price { get; set; }
        }

        private class Product
        {
            public string Name { get; set; }
            public string[] Images { get; set; }
            public double Price { get; set; }
        }

        private class Product2
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string[] Images { get; set; }
            public double Price { get; set; }
        }
    }
}
