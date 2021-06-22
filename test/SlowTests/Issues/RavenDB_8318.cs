using System;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Order = Tests.Infrastructure.Entities.Order;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8318 : RavenTestBase
    {
        public RavenDB_8318(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_patch_by_dynamic_query_with_filtering()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.Store(new Order
                    {
                        Company = "companies/1"
                    }, "orders/1");

                    session.Store(new Order(), "orders/2");

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = @"FROM Orders as o
WHERE o.Company != null
LOAD o.Company as company
UPDATE { o.Company = company.Name; } " }, new QueryOperationOptions()
                    {
                        StaleTimeout = TimeSpan.FromSeconds(60)
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");

                    Assert.Equal("HR", order.Company);

                    order = session.Load<Order>("orders/2");

                    Assert.Null(order.Company);
                }
            }
        }

        [Fact]
        public void Can_delete_by_dynamic_query_with_filtering()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");

                    session.Store(new Order
                    {
                        Company = "companies/1"
                    }, "orders/1");

                    session.Store(new Order(), "orders/2");

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new DeleteByQueryOperation(
                    new IndexQuery { Query = @"FROM Orders
WHERE Company = 'companies/1'" }, new QueryOperationOptions()
                    {
                        StaleTimeout = TimeSpan.FromSeconds(60)
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Order>("orders/1"));
                    Assert.NotNull(session.Load<Order>("orders/2"));
                }
            }
        }
    }
}
