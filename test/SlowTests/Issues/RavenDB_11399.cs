using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11399 : RavenTestBase
    {
        public RavenDB_11399(ITestOutputHelper output) : base(output)
        {
        }

        private class Employee
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string Title { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Anne",
                        Title = "Title"
                    });

                    session.Store(new Employee
                    {
                        FirstName = "Andrew",
                        Title = "Title"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var names = session.Advanced.RawQuery<Employee>("from Employees where Title != null and (boost(search(FirstName, 'andrew*'), 1000) or boost(search(FirstName, 'ann*'), 800)) order by score() select FirstName")
                        .NoCaching()
                        .ToArray()
                        .Select(x => x.FirstName)
                        .ToArray();
                    Assert.Equal("Andrew", names[0]);
                    Assert.Equal("Anne", names[1]);
                }

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/2-A");
                    employee.Title = "Title2";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var names = session.Advanced.RawQuery<Employee>("from Employees where Title != null and (boost(search(FirstName, 'andrew*'), 1000) or boost(search(FirstName, 'ann*'), 800)) order by score() select FirstName")
                        .NoCaching()
                        .ToArray()
                        .Select(x => x.FirstName)
                        .ToArray();

                    Assert.Equal("Andrew", names[0]);
                    Assert.Equal("Anne", names[1]);
                }
            }
        }
    }
}
