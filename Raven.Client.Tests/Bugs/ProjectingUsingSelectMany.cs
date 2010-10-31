using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class RavenDbDoesntDoSelectMany : LocalClientTest, IDisposable
    {
        private IDocumentStore store;

        public RavenDbDoesntDoSelectMany()
        {
            store = NewDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    OrderLines =
                        {
                            new OrderLine("Widget"),
                            new OrderLine("Gadget")
                        }
                });
                session.Store(new Order
                {
                    OrderLines =
                        {
                            new OrderLine("Fixit"),
                            new OrderLine("Gadget")
                        }
                });
                session.SaveChanges();
            }
        }

        [Fact]
        public void TryToSelectMany()
        {
            // failed: System.NotSupportedException : Method not supported: SelectMany
            using (var session = store.OpenSession())
            {
                var orderLines = session.Query<Order>().SelectMany(x => x.OrderLines);
                Assert.Equal(orderLines.ToArray().Length, 4);
            }
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }

    public class Order
    {
        public Order()
        {
            OrderLines = new List<OrderLine>();
        }

        public IList<OrderLine> OrderLines { get; private set; }
    }

    public class OrderLine
    {
        public OrderLine(string productName)
        {
            ProductName = productName;
        }

        public string ProductName { get; private set; }
    }
}