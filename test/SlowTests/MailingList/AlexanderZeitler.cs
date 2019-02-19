using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.MailingList
{
    public class AlexanderZeitler : RavenTestBase
    {
        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string[] SupplierIds { get; set; }
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

        [Fact]
        public void It_should_be_found()
        {
            var customer = new Customer()
            {
                Name = "ALFKI"
            };

            Order order = null;

            // red
            using (var store = GetDocumentStore())
            {
                // green
                //var documentStore = new DocumentStore()
                //{
                //    Url = "http://localhost:8080/databases/ravensubdocs"
                //}.Initialize();

                //documentStore.DatabaseCommands.DeleteByQyery("Raven/DocumentsByEntityName", new IndexQuery());


                using (var session = store.OpenSession())
                {
                    session.Store(customer);
                    session.SaveChanges();
                    order = new Order()
                    {
                        CustomerId = customer.Id,
                        TotalPrice = 200
                    };

                    session.Store(order);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    order = session.Include<Order>(x => x.CustomerId)
                        .Load(order.Id);

                    // this will not require querying the server!
                    customer = session.Load<Customer>(order.CustomerId);
                    Assert.NotEqual(null, customer);
                    Assert.Equal("ALFKI", customer.Name);
                }


                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Include(x => x.CustomerId)
                        // also try to comment this
                        .Where(x => x.TotalPrice > 100)
                        .ToList();

                    Assert.Equal(1, orders.Count);

                    foreach (var order1 in orders)
                    {
                        // this will not require querying the server!
                        var cust = session.Load<Customer>(order1.CustomerId);
                        Assert.NotEqual(null, cust);
                    }
                }
            }
        }
    }
}
