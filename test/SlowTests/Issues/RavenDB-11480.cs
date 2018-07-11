using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11480 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string OrderId { get; set; }
            }

            public Index1()
            {
                Map = companies => from c in companies
                    select new
                    {
                        OrderId = "orders/1-A"
                    };

                Store("OrderId", FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanLoadOnStoredField()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.Store(new Order
                    {
                        Company = "HR-Order"
                    }, "orders/1-A");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from c in session.Query<Index1.Result, Index1>()
                                let o = RavenQuery.Load<Order>(c.OrderId)
                                select o;

                    var order = query.First();

                    Assert.Equal("HR-Order", order.Company);
                }
            }
        }
    }
}
