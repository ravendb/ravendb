using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12638 : RavenTestBase
    {
        [Fact]
        public void Two_non_adjacent_recursive_queries_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var employee = new Employee
                        {
                            ReportsTo = "employees/" + (i + 1)
                        };
                        session.Store(employee, "employees/" + i);
                    }
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(
                        @"match (Employees as e) 
                            -recursive as r1 { [ReportsTo]->(Employees as manager) }-[ReportsTo]->(Employees as finalManager)
                            -recursive as r2 { [ReportsTo]->(Employees as manager2) }").ToList();

                    Assert.Equal(7, results.Count);
                }
            }
        }


        [Fact]
        public void Throws_when_using_two_adjacent_recursive_clauses()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                
                using (var session = store.OpenSession())
                {
                   var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<JObject>(
                        @"match (Employees as e) 
                            -recursive as r1 { [ReportsTo]->(Employees as m1) }
                            -recursive as r2 { [ReportsTo]->(Employees as m2) }").ToList());

                    Assert.True(e.Message.Contains("recursive") && e.Message.Contains("adjacent"));
                }
            }
        }
    }
}
