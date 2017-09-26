using System.Linq;
using FastTests;
using Orders;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8746 : RavenTestBase
    {
        [Fact]
        public void Can_use_alias_in_group_by()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "joe",
                    });

                    session.Store(new Employee
                    {
                        FirstName = "joe"
                    });

                    session.SaveChanges();

                    var names = session.Advanced.RawQuery<Result>("from Employees as e group by e.FirstName select e.FirstName, count()").ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Equal("joe", names[0].FirstName);
                    Assert.Equal(2, names[0].Count);
                }
            }
        }

        private class Result
        {
            public string FirstName { get; set; }

            public int Count { get; set; }
        }
    }
}
