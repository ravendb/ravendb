using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16145 : RavenTestBase
    {
        public RavenDB_16145(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Indexing_Should_Throw_Error_If_LoadDocument_Collection_Does_Not_Match()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    var employee = new Employee { FirstName = "John", LastName = "Doe" };
                    session.Store(employee);

                    session.Store(new Company { Name = "HR", ExternalId = employee.Id });
                    session.SaveChanges();
                }

                var errors = WaitForIndexingErrors(store);
                Assert.Equal(1, errors.Length);
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains("Cannot load document 'employees/1-a', because it's collection 'Employees' does not match the requested collection name 'Addresses'.", errors[0].Errors[0].Error);
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from company in companies
                                   let employee = LoadDocument<Employee>(company.ExternalId, "Addresses")
                                   select new
                                   {
                                       Name = company.Name
                                   };
            }
        }
    }
}
