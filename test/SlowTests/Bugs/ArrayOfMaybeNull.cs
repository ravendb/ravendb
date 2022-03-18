using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class ArrayOfMaybeNull : RavenTestBase
    {
        public ArrayOfMaybeNull(ITestOutputHelper output) : base(output)
        {
        }

        private class Orders_Search : AbstractIndexCreationTask<Order>
        {
            public class ReduceResult
            {
                public string Query { get; set; }
                public DateTime OrderedAt { get; set; }
            }

            public Orders_Search()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    Query = new[]
                                    {
                                        order.FirstName,
                                        order.LastName,
                                        order.OrderNumber,
                                        order.Email,
                                        order.CompanyName
                                    },
                                    order.OrderedAt
                                };
            }
        }

        private class Order
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string OrderNumber { get; set; }
            public string Email { get; set; }
            public string CompanyName { get; set; }
            public DateTime OrderedAt { get; set; }
        }

        [Fact]
        public async Task CanPerformSearch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        CompanyName = null,
                        Email = "ayende@ayende.com",
                        FirstName = "Oren",
                        LastName = "Eini",
                        OrderNumber = "E12312",
                        OrderedAt = DateTime.Now
                    });
                    session.SaveChanges();
                }

                new Orders_Search().Execute(store);

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Orders_Search.ReduceResult, Orders_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Query == "oren")
                        .As<Order>()
                        .ToList();

                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                    Assert.Equal(errorsCount, 0);

                    Assert.NotEmpty(orders);
                }
            }
        }
    }
}
