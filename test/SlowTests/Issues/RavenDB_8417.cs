using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Orders;
using Xunit;
using Raven.Client.Documents.Linq;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8417 : RavenTestBase
    {
        public RavenDB_8417(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TotalResultsShouldBeCountedProperlyForCollectionQueries()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Employee>()
                        .Statistics(out var stats)
                        .Where(x => x.Id == "employees/1-A")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.NotNull(results[0]);
                    Assert.Equal(1, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Where(x => x.Id == "employees/1-A")
                        .Skip(1)
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Where(x => x.Id.In("employees/1-A", "do-not-exist"))
                        .Skip(1)
                        .ToList();

                    Assert.Equal(0, results.Count);
                    Assert.Equal(1, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Where(x => x.Id.In("employees/1-A", "employees/2-A", "employees/3-A", "do-not-exist"))
                        .Skip(0)
                        .Take(1)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(3, stats.TotalResults);

                    results = session.Query<Employee>()
                        .Statistics(out stats)
                        .Skip(1)
                        .ToList();

                    Assert.Equal(8, results.Count);
                    Assert.Equal(9, stats.TotalResults);

                    var allResults = session.Query<object>()
                        .Statistics(out stats)
                        .Skip(1)
                        .Take(1)
                        .ToList();

                    Assert.Equal(1, allResults.Count);
                    Assert.Equal(1059, stats.TotalResults);
                }
            }
        }
    }
}
