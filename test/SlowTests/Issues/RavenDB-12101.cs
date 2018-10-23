using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12101 : RavenTestBase
    {
        private class CompaniesByEmployees : AbstractIndexCreationTask<Company>
        {
            public CompaniesByEmployees()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                     Employees = company.Employees
                                   };
            }

            public class Result
            {
                public string[] Employees { get; set; }

            }
        }

        private class Company
        {
            public IEnumerable<string> Employees { get; set; }
        }

        [Fact]
        public void IndexQueryWithStringEqualsInsideWhereAnyShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                new CompaniesByEmployees().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Employees = new []{ "employees/1", "employees/2" }
                    });
                    session.Store(new Company
                    {
                        Employees = new[] { "employees/2", "employees/3" }
                    });
                    session.Store(new Company
                    {
                        Employees = new[] { "employees/3", "employees/1" }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<CompaniesByEmployees.Result, CompaniesByEmployees>()
                        .Where(x => x.Employees.Any(y => y.Equals("employees/1")));

                     Assert.Equal("from index 'CompaniesByEmployees' where Employees = $p0", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                }
            }
        }
    }
}
