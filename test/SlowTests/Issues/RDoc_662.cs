using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDoc_662 : RavenTestBase
    {
        private class Employees_Query : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public string[] Query { get; set; }
            }

            public Employees_Query()
            {
                Map = employees => from employee in employees
                    select new
                    {
                        Query = new[]
                        {
                            employee.FirstName,
                            employee.LastName,
                            employee.Title,
                            employee.Address.City
                        }
                    };

                Index("Query", FieldIndexing.Search);
            }
        }

        [Fact]
        public async Task OfTypeShouldWorkInDocumentQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                new Employees_Query().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var employees = session
                        .Query<Employees_Query.Result, Employees_Query>()
                        .Search(x => x.Query, "King")
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, employees.Count);

                    var employee = session.Load<Employee>(employees[0].Id);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(employee, employees[0]);
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var employees = session
                        .Advanced
                        .DocumentQuery<Employees_Query.Result, Employees_Query>()
                        .Search(x => x.Query, "King")
                        .OfType<Employee>()
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, employees.Count);

                    var employee = session.Load<Employee>(employees[0].Id);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(employee, employees[0]);
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var employees = await session
                        .Advanced
                        .AsyncDocumentQuery<Employees_Query.Result, Employees_Query>()
                        .Search(x => x.Query, "King")
                        .OfType<Employee>()
                        .ToListAsync();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(1, employees.Count);

                    var employee = await session.LoadAsync<Employee>(employees[0].Id);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(employee, employees[0]);
                }
            }
        }
    }
}