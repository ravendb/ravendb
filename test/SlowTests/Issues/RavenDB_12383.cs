using FastTests;
using Orders;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12383 : RavenTestBase
    {
        [Fact]
        public void IncludeShouldSkipDocumentsThatArePartOfResults()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e1 = new Employee
                    {
                        FirstName = "John"
                    };

                    session.Store(e1, "employees/1");

                    var e2 = new Employee
                    {
                        FirstName = "Edward",
                        ReportsTo = e1.Id
                    };

                    session.Store(e2, "employees/2");

                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var getDocumentsCommand = new GetDocumentsCommand(
                        new[] { "employees/1", "employees/2" },
                        new[] { nameof(Employee.ReportsTo) },
                        metadataOnly: false);

                    commands.Execute(getDocumentsCommand);

                    var results = getDocumentsCommand.Result.Results;
                    var includes = getDocumentsCommand.Result.Includes;

                    Assert.Equal(2, results.Length);
                    Assert.Equal(0, includes.Count);

                    var result = commands.Query(new IndexQuery
                    {
                        Query = "from Employees include ReportsTo"
                    });

                    results = result.Results;
                    includes = result.Includes;

                    Assert.Equal(2, results.Length);
                    Assert.Equal(0, includes.Count);

                    result = commands.Query(new IndexQuery
                    {
                        Query = @"
declare function f(d) {
    include(d.ReportsTo);
    return d;
}
from Employees as e select f(e)"
                    });

                    results = result.Results;
                    includes = result.Includes;

                    Assert.Equal(2, results.Length);
                    Assert.Equal(0, includes.Count);
                }
            }
        }
    }
}
