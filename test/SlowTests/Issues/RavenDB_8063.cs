using FastTests;
using Orders;
using Raven.Client.Documents.Commands;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8063 : RavenTestBase
    {
        [Fact]
        public void IncludeShouldNotReturnDuplicates()
        {
            using (var store = GetDocumentStore())
            {
                string orderId;

                using (var session = store.OpenSession())
                {
                    var employee = new Employee()
                    {
                        FirstName = "John"
                    };

                    session.Store(employee);

                    var order = new Order
                    {
                        Employee = employee.Id
                    };

                    session.Store(order);

                    orderId = order.Id;

                    session.SaveChanges();
                }

                var command = new GetDocumentsCommand(new[] { orderId }, new[] { "Employee" }, false);
                using (var commands = store.Commands())
                {
                    commands.RequestExecutor.Execute(command, commands.Context);

                    var result = command.Result;
                    Assert.Equal(1, result.Includes.Count);
                }
            }
        }
    }
}
