using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11701 : RavenTestBase
    {
        public RavenDB_11701(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RawQueryIncludeCounterByPropertyWithoutAliasNotation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<Order>("from Orders include counters(Employee, $p0)")
                        .AddParameter("p0", "Downloads")
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var val = session.CountersFor("employees/1-A").Get("Downloads");
                    Assert.Equal(100, val);

                    val = session.CountersFor("employees/2-A").Get("Downloads");
                    Assert.Equal(200, val);

                    val = session.CountersFor("employees/3-A").Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }
    }
}
