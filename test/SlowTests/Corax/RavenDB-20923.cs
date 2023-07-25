using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_20923 : RavenTestBase
{
    public RavenDB_20923(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void SortingBigSetReturnsCorrectOrder()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var expectedValues = Enumerable.Range(0, 10_000).Select(i => new Dto(-2500D + (i * 0.5D))).ToList();

        using (var bulkInsert = store.BulkInsert())
        {
            foreach (var expected in expectedValues)
                bulkInsert.Store(expected);
        }

        using var session = store.OpenSession();
        var sortedByCorax = session.Advanced.DocumentQuery<Dto>().OrderBy(i => i.DoubleValue, OrderingType.Double).WaitForNonStaleResults().ToList();
        Assert.Equal(expectedValues.OrderBy(i => i.DoubleValue).Select(i => i.Id), sortedByCorax.Select(i => i.Id));
    }

    private record Dto(double DoubleValue, string Id = null);
}
