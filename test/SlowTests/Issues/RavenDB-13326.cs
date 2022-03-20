using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Queries;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13326 : RavenTestBase
    {
        public RavenDB_13326(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Project_With_Multiple_Nested_Loads_Executes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var category = new Category { Name = "Food" };
                    session.Store(category);

                    var customer = new Customer { Name = "John Doe" };
                    session.Store(customer);

                    var order = new Order
                    {
                        CustomerId = customer.Id,
                        Items = new[] { new OrderItem { Name = "Bananas", CategoryId = category.Id } },
                        Instructions = new[] { new Instruction { Title = "Eat bananas" } }
                    };
                    session.Store(order);

                    var invoice = new Invoice { Orders = new[] { new OrderReference { Id = order.Id } } };
                    session.Store(invoice);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from i in session.Query<Invoice>()
                                select new
                                {
                                    i.Id,
                                    Orders = from orderRef in i.Orders
                                             let order = RavenQuery.Load<Order>(orderRef.Id)
                                             let customer = RavenQuery.Load<Customer>(order.CustomerId)
                                             select new
                                             {
                                                 order.Id,
                                                 CustomerName = customer.Name,
                                                 Items = from item in order.Items
                                                         let category = RavenQuery.Load<Category>(item.CategoryId)
                                                         select new
                                                         {
                                                             ItemName = item.Name,
                                                             CategoryName = category.Name
                                                         },
                                                 Instructions = from instruction in order.Instructions
                                                                select new
                                                                {
                                                                    instruction.Title
                                                                }
                                             }
                                };

                    var result = query.ToList();

                    Assert.NotNull(result);

                    Assert.Equal(1, result.Count);
                    Assert.Equal("invoices/1-A", result[0].Id);

                    var orders = result[0].Orders.ToList();
                    Assert.Equal(1, orders.Count);
                    Assert.Equal("orders/1-A", orders[0].Id);
                    Assert.Equal("John Doe", orders[0].CustomerName);

                    var items = orders[0].Items.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("Bananas", items[0].ItemName);
                    Assert.Equal("Food", items[0].CategoryName);

                    var instructions = orders[0].Instructions.ToList();
                    Assert.Equal(1, instructions.Count);
                    Assert.Equal("Eat bananas", instructions[0].Title);

                }
            }
        }

        [Fact]
        public void Project_With_Multiple_Nested_Lets_And_Loads_Executes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var category = new Category { Name = "Food" };
                    session.Store(category);

                    var customer = new Customer { Name = "John Doe" };
                    session.Store(customer);

                    var order = new Order
                    {
                        CustomerId = customer.Id,
                        Items = new[] { new OrderItem { Name = "Bananas", CategoryId = category.Id } },
                        Instructions = new[] { new Instruction { Title = "Eat bananas" } }
                    };
                    session.Store(order);

                    var invoice = new Invoice { Orders = new[] { new OrderReference { Id = order.Id } } };
                    session.Store(invoice);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from i in session.Query<Invoice>()
                                select new
                                {
                                    i.Id,
                                    Orders = from orderRef in i.Orders
                                             let order = RavenQuery.Load<Order>(orderRef.Id)
                                             let customer = RavenQuery.Load<Customer>(order.CustomerId)
                                             let test = "test"
                                             let test2 = "test2"
                                             select new
                                             {
                                                 order.Id,
                                                 CustomerName = customer.Name,
                                                 Items = from item in order.Items
                                                         let category = RavenQuery.Load<Category>(item.CategoryId)
                                                         let test3 = "test3"
                                                         let test4 = "test4"
                                                         select new
                                                         {
                                                             ItemName = item.Name,
                                                             CategoryName = category.Name,
                                                             Test = test.Substring(2),
                                                             Test2 = test2.Substring(2),
                                                             Test3 = test3.Substring(2),
                                                             Test4 = test4.Substring(2)
                                                         },
                                                 Instructions = from instruction in order.Instructions
                                                                let test5 = "test5"
                                                                let test6 = "test6"
                                                                select new
                                                                {
                                                                    instruction.Title,
                                                                    Test5 = test5.Substring(2),
                                                                    Test6 = test6.Substring(2)
                                                                }
                                             }
                                };

                    var result = query.ToList();

                    Assert.NotNull(result);

                    Assert.Equal(1, result.Count);
                    Assert.Equal("invoices/1-A", result[0].Id);

                    var orders = result[0].Orders.ToList();
                    Assert.Equal(1, orders.Count);
                    Assert.Equal("orders/1-A", orders[0].Id);
                    Assert.Equal("John Doe", orders[0].CustomerName);

                    var items = orders[0].Items.ToList();
                    Assert.Equal(1, items.Count);
                    Assert.Equal("Bananas", items[0].ItemName);
                    Assert.Equal("Food", items[0].CategoryName);

                    Assert.Equal("st", items[0].Test);
                    Assert.Equal("st2", items[0].Test2);
                    Assert.Equal("st3", items[0].Test3);
                    Assert.Equal("st4", items[0].Test4);

                    var instructions = orders[0].Instructions.ToList();
                    Assert.Equal(1, instructions.Count);
                    Assert.Equal("Eat bananas", instructions[0].Title);
                    Assert.Equal("st5", instructions[0].Test5);
                    Assert.Equal("st6", instructions[0].Test6);

                }
            }
        }

        private class Invoice
        {
            public string Id { get; set; }
            public IEnumerable<OrderReference> Orders { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public IEnumerable<OrderItem> Items { get; set; }
            public IEnumerable<Instruction> Instructions { get; set; }
        }

        private class OrderReference
        {
            public string Id { get; set; }
        }

        private class OrderItem
        {
            public string Name { get; set; }
            public string CategoryId { get; set; }
        }

        private class Instruction
        {
            public string Title { get; set; }
        }

        private class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
