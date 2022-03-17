using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RustyTests : RavenTestBase
    {
        public RustyTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Projection_With_Multiple_Nested_Loads_Should_Not_Throw_Exception()
        {
            //https://10137960102884337581.googlegroups.com/attach/a47ae1f7c46d8/ProjectionNestedLoadExceptionTests.cs?part=0.2&view=1&vt=ANaJVrG8D8tAgKI01vVR3y1j2c8-6Mul6tmESdLbo0YG3w3ovm7Tzb1YQN5Rl-IDNc8l249MB40HXW76RnFBjd8irKij28ZAGFo2_Tqv9mGobBqeq7xYg8w

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var revenueStream = new RevenueStream { Name = "Catering" };
                    session.Store(revenueStream);

                    var category = new Category { Name = "Food" };
                    session.Store(category);

                    var item1 = new Item { Name = "Olives", CategoryId = category.Id, RevenueStreamId = revenueStream.Id };
                    session.Store(item1);

                    var item2 = new Item { Name = "Lamb", CategoryId = category.Id, RevenueStreamId = revenueStream.Id };
                    session.Store(item2);

                    var order = new Order
                    {
                        OrderItems = new[]
                        {
                            new OrderItem { ItemId = item1.Id, Price = 100 },
                            new OrderItem { ItemId = item2.Id, Price = 50 }
                        }
                    };
                    session.Store(order);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = (from o in session.Query<Order>()
                                  select new OrderProjection
                                  {
                                      OrderId = o.Id,
                                      OrderItems = from i in o.OrderItems
                                                   let item = RavenQuery.Load<Item>(i.ItemId)
                                                   let category = RavenQuery.Load<Category>(item.CategoryId)
                                                   let revenueStream = RavenQuery.Load<RevenueStream>(item.RevenueStreamId)
                                                   select new OrderItemProjection
                                                   {
                                                       ItemId = i.ItemId,
                                                       Price = i.Price,
                                                       ItemName = item.Name,
                                                       CategoryName = category.Name,
                                                       RevenueStreamName = revenueStream.Name
                                                   }
                                  }).Single();

                    Assert.NotNull(result);
                }
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public IList<OrderItem> OrderItems { get; set; }
        }

        private class OrderItem
        {
            public string ItemId { get; set; }
            public decimal Price { get; set; }
        }

        private class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CategoryId { get; set; }
            public string RevenueStreamId { get; set; }
        }

        private class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class RevenueStream
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class OrderProjection
        {
            public string OrderId { get; set; }
            public IEnumerable<OrderItemProjection> OrderItems { get; set; }
        }

        private class OrderItemProjection
        {
            public string ItemId { get; set; }
            public string ItemName { get; set; }
            public decimal Price { get; set; }
            public string CategoryName { get; set; }
            public string RevenueStreamName { get; set; }
        }
    }
}
