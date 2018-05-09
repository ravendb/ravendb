using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11009 : RavenTestBase
    {
        [Fact]
        public void ShouldDoCaseSensitiveAutoIndexLookup()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order()
                    {
                        Employee = "employees/1-A",
                        Company = "companies/1-A"
                    });

                    session.SaveChanges();

                   session.Advanced.RawQuery<dynamic>(@"from Orders
group by employee, company
select employee as employeeIdentifier, company, count()").Statistics(out var stats).ToList();
                    
                    Assert.Equal("Auto/Orders/ByCountReducedBycompanyAndemployee", stats.IndexName);

                    var results = session.Advanced.RawQuery<Result>(@"from Orders
group by Employee, Company
select Employee as employeeIdentifier, Company, count()").Statistics(out stats).ToList();

                    
                    Assert.Equal("Auto/Orders/ByCountReducedByCompanyAndEmployee", stats.IndexName);

                    Assert.Equal(1, results[0].Count);
                    Assert.Equal("employees/1-A", results[0].employeeIdentifier);
                    Assert.Equal("companies/1-A", results[0].Company);

                    var indexStats = store.Maintenance.Send(new GetIndexesStatisticsOperation());

                    Assert.Equal(2, indexStats.Length);
                }
            }
        }

        private class Result
        {
            public string employeeIdentifier { get; set; }
            public string Company { get; set; }
            public int Count { get; set; }
        }
    }
}
