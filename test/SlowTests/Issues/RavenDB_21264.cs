using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21264 : RavenTestBase
{
    public RavenDB_21264(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ShouldSetSkipStatisticsAccordingly()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var indexQuery = session.Advanced.AsyncDocumentQuery<Employee>()
                    .WhereStartsWith(p => p.FirstName, "bob")
                    .OrderBy(x => x.Birthday)
                    .GetIndexQuery();
                
                Assert.True(indexQuery.SkipStatistics);
            }

            using (var session = store.OpenSession())
            {
                var indexQuery = session.Advanced.DocumentQuery<Employee>()
                    .Statistics(out var stats)
                    .WhereStartsWith(p => p.FirstName, "bob")
                    .OrderBy(x => x.Birthday)
                    .GetIndexQuery();

                Assert.False(indexQuery.SkipStatistics);
            }
        }
    }
}
