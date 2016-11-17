using System;
using System.Globalization;
using System.Linq;
using Raven.NewClient.Client.Document;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace NewClientTests.NewClient
{
    public class Includes :  RavenTestBase
    {
        [Fact(Skip = "NotImplementedException")]
        public void Can_Load_With_Include()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var address = new Address { City = "London", Country = "UK" };
                    session.Store(address);
                    session.Store(new User { Name = "Adam", AddressId = session.Advanced.GetDocumentId(address) });

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Include<User>(x => x.AddressId).Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Include("AddressId").Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_Use_Includes_Within_Multi_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer {Id = "users/1", Name = "Daniel Lang"});
                    session.Store(new Customer {Id = "users/2", Name = "Oren Eini"});

                    session.Store(new Order {CustomerId = "users/1", Number = "1"});
                    session.Store(new Order {CustomerId = "users/1", Number = "2"});
                    session.Store(new Order {CustomerId = "users/2", Number = "3"});

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var orders = session.Query<Order>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                        .Include(x => x.CustomerId)
                        .ToList();

                    Assert.Equal(3, orders.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var customers = session.Load<Customer>(orders.Select(x => x.CustomerId));
                    Assert.Equal(3, customers.Length);
                    Assert.Equal(2, customers.Distinct().Count());
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Primary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer {Id = "customers/1"});
                    session.Store(new Order {CustomerId = "customers/1"}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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
        public void Can_Include_By_Primary_Valuetype_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer2 {Id = 1});
                    session.Store(new Order2 {Customer2Id = 1}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Customer2Id)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer2>(order.Customer2Id);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Primary_Valuetype_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer2 {Id = 1});
                    session.Store(new Order2 {Customer2Id = 1}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Customer2IdString)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer2>(order.Customer2Id);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Query_With_Include_By_Primary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer {Id = "customers/1", Name = "1"});
                    session.Store(new Customer {Id = "customers/2", Name = "2"});
                    session.Store(new Customer {Id = "customers/3", Name = "3"});
                    session.Store(new Order {CustomerId = "customers/1", TotalPrice = 200D}, "orders/1234");
                    session.Store(new Order {CustomerId = "customers/2", TotalPrice = 50D}, "orders/1235");
                    session.Store(new Order {CustomerId = "customers/3", TotalPrice = 300D}, "orders/1236");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var orders = session.Query<Order>()
                        .Customize(x => x.Include<Order>(o => o.CustomerId))
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
        public void Can_Query_With_Include_By_Primary_Valuetype_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer2 {Id = 1, Name = "1"});
                    session.Store(new Customer2 {Id = 2, Name = "2"});
                    session.Store(new Customer2 {Id = 3, Name = "3"});
                    session.Store(new Order2 {Customer2Id = 1, TotalPrice = 200D}, "orders/1234");
                    session.Store(new Order2 {Customer2Id = 2, TotalPrice = 50D}, "orders/1235");
                    session.Store(new Order2 {Customer2Id = 3, TotalPrice = 300D}, "orders/1236");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var orders = session.Query<Order2>()
                        .Customize(x => x.Include<Order2, Customer2>(o => o.Customer2Id))
                        .Where(x => x.TotalPrice > 100)
                        .ToList();

                    Assert.Equal(2, orders.Count);

                    foreach (var order in orders)
                    {
                        // this will not require querying the server!
                        var cust = session.Load<Customer2>(order.Customer2Id);
                        Assert.NotNull(cust);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_Include_By_Primary_List_Of_Strings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Supplier {Name = "1"});
                    session.Store(new Supplier {Name = "2"});
                    session.Store(new Supplier {Name = "3"});
                    session.Store(new Order {SupplierIds = new[] {"suppliers/1", "suppliers/2", "suppliers/3"}},
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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

        [Fact]
        public void Can_Include_By_Primary_List_Of_Valuetypes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var guid1 = Guid.NewGuid();
                    var guid2 = Guid.NewGuid();
                    var guid3 = Guid.NewGuid();
                    session.Store(new Supplier2 {Id = guid1, Name = "1"});
                    session.Store(new Supplier2 {Id = guid2, Name = "2"});
                    session.Store(new Supplier2 {Id = guid3, Name = "3"});
                    session.Store(new Order2 {Supplier2Ids = new[] {guid1, guid2, guid3}}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order2, Supplier2>(x => x.Supplier2Ids)
                        .Load("orders/1234");

                    Assert.Equal(3, order.Supplier2Ids.Count());

                    foreach (var supplier2Id in order.Supplier2Ids)
                    {
                        // this will not require querying the server!
                        var supp2 = session.Load<Supplier2>(supplier2Id);
                        Assert.NotNull(supp2);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_Include_By_Secondary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer());
                    session.Store(new Order {Refferal = new Referral {CustomerId = "customers/1"}}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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
        public void Can_Include_By_Secondary_Valuetype_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer2 {Id = 1});
                    session.Store(new Order2 {Refferal2 = new Referral2 {Customer2Id = 1}}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Refferal2.Customer2Id)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var referrer2 = session.Load<Customer2>(order.Refferal2.Customer2Id);

                    Assert.NotNull(referrer2);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void Can_Include_By_List_Of_Secondary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product {Name = "1"});
                    session.Store(new Product {Name = "2"});
                    session.Store(new Product {Name = "3"});
                    session.Store(
                        new Order
                        {
                            LineItems =
                                new[]
                                {
                                    new LineItem {ProductId = "products/1"}, new LineItem {ProductId = "products/2"},
                                    new LineItem {ProductId = "products/3"}
                                }
                        }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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

        [Fact]
        public void Can_Include_By_List_Of_Secondary_Valuetype_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var guid1 = Guid.NewGuid();
                    var guid2 = Guid.NewGuid();
                    var guid3 = Guid.NewGuid();
                    session.Store(new Product2 {Id = guid1, Name = "1"});
                    session.Store(new Product2 {Id = guid2, Name = "2"});
                    session.Store(new Product2 {Id = guid3, Name = "3"});
                    session.Store(
                        new Order2
                        {
                            LineItem2s =
                                new[]
                                {
                                    new LineItem2 {Product2Id = guid1}, new LineItem2 {Product2Id = guid2},
                                    new LineItem2 {Product2Id = guid3}
                                }
                        }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order2, Product2>(x => x.LineItem2s.Select(li => li.Product2Id))
                        .Load("orders/1234");

                    foreach (var lineItem2 in order.LineItem2s)
                    {
                        // this will not require querying the server!
                        var product2 = session.Load<Product2>(lineItem2.Product2Id);
                        Assert.NotNull(product2);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void Can_Include_By_Denormalized_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Customer2 {Id = 1});
                    session.Store(new Order3 {Customer = new DenormalizedCustomer {Id = 1}}, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var order = session.Include<Order3, Customer2>(x => x.Customer.Id)
                        .Load("orders/1234");

                    // this will not require querying the server!
                    var fullCustomer = session.Load<Customer2>(order.Customer.Id);

                    Assert.NotNull(fullCustomer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        public class Order
        {
            public string Number { get; set; }
            public string CustomerId { get; set; }
            public string[] SupplierIds { get; set; }
            public Referral Refferal { get; set; }
            public LineItem[] LineItems { get; set; }
            public double TotalPrice { get; set; }
        }

        public class Order2
        {
            public int Customer2Id { get; set; }
            public string Customer2IdString { get { return Customer2Id.ToString(CultureInfo.InvariantCulture); } }
            public Guid[] Supplier2Ids { get; set; }
            public Referral2 Refferal2 { get; set; }
            public LineItem2[] LineItem2s { get; set; }
            public double TotalPrice { get; set; }
        }

        public class Order3
        {
            public DenormalizedCustomer Customer { get; set; }
            public string[] SupplierIds { get; set; }
            public Referral Refferal { get; set; }
            public LineItem[] LineItems { get; set; }
            public double TotalPrice { get; set; }
        }

        public class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public short Age { get; set; }
            public string HashedPassword { get; set; }
        }

        public class Customer2
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public short Age { get; set; }
            public string HashedPassword { get; set; }
        }

        public class DenormalizedCustomer
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Supplier
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Supplier2
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Referral
        {
            public string CustomerId { get; set; }
            public double CommissionPercentage { get; set; }
        }

        public class Referral2
        {
            public int Customer2Id { get; set; }
            public double CommissionPercentage { get; set; }
        }

        public class LineItem
        {
            public string ProductId { get; set; }
            public string Name { get; set; }
            public int Quantity { get; set; }
            public double Price { get; set; }
        }

        public class LineItem2
        {
            public Guid Product2Id { get; set; }
            public string Name { get; set; }
            public int Quantity { get; set; }
            public double Price { get; set; }
        }

        public class Product
        {
            public string Name { get; set; }
            public string[] Images { get; set; }
            public double Price { get; set; }
        }

        public class Product2
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string[] Images { get; set; }
            public double Price { get; set; }
        }
    }
}