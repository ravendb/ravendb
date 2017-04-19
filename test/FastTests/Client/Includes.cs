using System;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class Includes : RavenTestBase
    {
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
                    var user = session.Include<User>(x => x.AddressId).Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }

                using (var session = store.OpenSession())
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

        [Fact]
        public void Can_Use_Includes_Within_Multi_Load()
        {
            using (var store = GetDocumentStore())
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
        public void Can_Query_With_Include_By_Primary_String_Property()
        {
            using (var store = GetDocumentStore())
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
        public void Can_Include_By_Primary_List_Of_Strings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Supplier { Name = "1" });
                    session.Store(new Supplier { Name = "2" });
                    session.Store(new Supplier { Name = "3" });
                    session.Store(new Order { SupplierIds = new[] { "suppliers/1", "suppliers/2", "suppliers/3" } },
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

        [Fact]
        public void Can_Include_By_Secondary_String_Property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer());
                    session.Store(new Order { Refferal = new Referral { CustomerId = "customers/1" } }, "orders/1234");

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
                                    new LineItem {ProductId = "products/1"}, new LineItem {ProductId = "products/2"},
                                    new LineItem {ProductId = "products/3"}
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