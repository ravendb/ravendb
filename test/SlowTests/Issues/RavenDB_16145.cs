using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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
        public void Indexing_Should_Not_Throw_Error_If_LoadDocument_Collection_Does_Not_Match_It_Should_Return_Null_Instead()
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

                WaitForIndexing(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(new Companies_ByName().IndexName, "Name", null));

                Assert.Equal(1, terms.Length);
                Assert.Equal("hr", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(new Companies_ByName().IndexName, "Employee", null));

                Assert.Equal(0, terms.Length);
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
                                       Name = company.Name,
                                       Employee = employee.FirstName
                                   };
            }
        }
    }
}
