using System;
using System.Linq;

using FastTests;
using Raven.NewClient.Client;
using Xunit;

namespace SlowTests.Tests.Queries
{
    public class Includes : RavenNewTestBase
    {
        [Fact]
        public void Can_use_includes_within_multi_load()
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
                    var orders =
                        session.Query<Order>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Include(x => x.CustomerId)
                            .ToList();

                    Assert.Equal(3, orders.Count);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var customers = session.Load<Customer>(orders.Select(x => x.CustomerId));
                    Assert.Equal(2, customers.Count);
                    Assert.Equal(2, customers.Distinct().Count());
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_include_by_primary_string_property()
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
                    var order = session.Include<Order>(x => x.CustomerId).Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer>(order.CustomerId);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_include_by_primary_valuetype_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer2 { Id = "customers/1" });
                    session.Store(new Order2 { Customer2Id = "customers/1" }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Customer2Id).Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer2>(order.Customer2Id);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_include_by_primary_valuetype_string_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer2 { Id = "customers/1" });
                    session.Store(new Order2 { Customer2Id = "customers/1" }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Customer2IdString).Load("orders/1234");

                    // this will not require querying the server!
                    var cust = session.Load<Customer2>(order.Customer2Id);

                    Assert.NotNull(cust);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_query_with_include_by_primary_string_property()
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
                    var orders =
                        session.Query<Order>()
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
        public void can_query_with_include_by_primary_valuetype_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer2 { Id = "customers/1", Name = "1" });
                    session.Store(new Customer2 { Id = "customers/2", Name = "2" });
                    session.Store(new Customer2 { Id = "customers/3", Name = "3" });
                    session.Store(new Order2 { Customer2Id = "customers/1", TotalPrice = 200D }, "orders/1234");
                    session.Store(new Order2 { Customer2Id = "customers/2", TotalPrice = 50D }, "orders/1235");
                    session.Store(new Order2 { Customer2Id = "customers/3", TotalPrice = 300D }, "orders/1236");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders =
                        session.Query<Order2>()
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

        [Fact]
        public void can_include_by_primary_list_of_strings()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Supplier { Name = "1" });
                    session.Store(new Supplier { Name = "2" });
                    session.Store(new Supplier { Name = "3" });
                    session.Store(
                        new Order { SupplierIds = new[] { "suppliers/1", "suppliers/2", "suppliers/3" } },
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.SupplierIds).Load("orders/1234");

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
        public void can_include_by_primary_list_of_valuetypes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var guid1 = Guid.NewGuid().ToString();
                    var guid2 = Guid.NewGuid().ToString();
                    var guid3 = Guid.NewGuid().ToString();
                    session.Store(new Supplier2 { Id = guid1, Name = "1" });
                    session.Store(new Supplier2 { Id = guid2, Name = "2" });
                    session.Store(new Supplier2 { Id = guid3, Name = "3" });
                    session.Store(new Order2 { Supplier2Ids = new[] { guid1, guid2, guid3 } }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order2, Supplier2>(x => x.Supplier2Ids).Load("orders/1234");

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

        [Fact]
        public void can_include_by_secondary_string_property()
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
                    var order = session.Include<Order>(x => x.Refferal.CustomerId).Load("orders/1234");

                    // this will not require querying the server!
                    var referrer = session.Load<Customer>(order.Refferal.CustomerId);

                    Assert.NotNull(referrer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_include_by_secondary_valuetype_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer2 { Id = "customers/1" });
                    session.Store(new Order2 { Refferal2 = new Referral2 { Customer2Id = "customers/1" } }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order2, Customer2>(x => x.Refferal2.Customer2Id).Load("orders/1234");

                    // this will not require querying the server!
                    var referrer2 = session.Load<Customer2>(order.Refferal2.Customer2Id);

                    Assert.NotNull(referrer2);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void can_include_by_list_of_secondary_string_property()
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
                                            new LineItem { ProductId = "products/1" },
                                            new LineItem { ProductId = "products/2" },
                                            new LineItem { ProductId = "products/3" }
                                        }
                        },
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order>(x => x.LineItems.Select(li => li.ProductId)).Load("orders/1234");

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
        public void can_include_by_list_of_secondary_valuetype_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var guid1 = Guid.NewGuid().ToString();
                    var guid2 = Guid.NewGuid().ToString();
                    var guid3 = Guid.NewGuid().ToString();
                    session.Store(new Product2 { Id = guid1, Name = "1" });
                    session.Store(new Product2 { Id = guid2, Name = "2" });
                    session.Store(new Product2 { Id = guid3, Name = "3" });
                    session.Store(
                        new Order2
                        {
                            LineItem2s =
                                    new[]
                                        {
                                            new LineItem2 { Product2Id = guid1 },
                                            new LineItem2 { Product2Id = guid2 },
                                            new LineItem2 { Product2Id = guid3 }
                                        }
                        },
                        "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order =
                        session.Include<Order2, Product2>(x => x.LineItem2s.Select(li => li.Product2Id))
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
        public void can_include_by_denormalized_property()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer2 { Id = "customers/1" });
                    session.Store(new Order3 { Customer = new DenormalizedCustomer { Id = "customers/1" } }, "orders/1234");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Include<Order3, Customer2>(x => x.Customer.Id).Load("orders/1234");

                    // this will not require querying the server!
                    var fullCustomer = session.Load<Customer2>(order.Customer.Id);

                    Assert.NotNull(fullCustomer);
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
            public string Customer2Id { get; set; }
            public string Customer2IdString => Customer2Id;
            public string[] Supplier2Ids { get; set; }
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
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public short Age { get; set; }
            public string HashedPassword { get; set; }
        }

        private class DenormalizedCustomer
        {
            public string Id { get; set; }
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
            public string Id { get; set; }
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
            public string Customer2Id { get; set; }
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
            public string Product2Id { get; set; }
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
            public string Id { get; set; }
            public string Name { get; set; }
            public string[] Images { get; set; }
            public double Price { get; set; }
        }
    }
}
