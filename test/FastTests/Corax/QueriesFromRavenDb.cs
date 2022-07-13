using System.Linq;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class QueriesFromRavenDb : RavenTestBase
{
    public QueriesFromRavenDb(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanMixValuesInQueryForBetween(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new DoubleItem(2));
            s.SaveChanges();
        }

        {
            using var s = store.OpenSession();
            var q = s.Advanced.RawQuery<DoubleItem>("from DoubleItems where 'Value' < 3 and 'Value' > 1.5").ToList();
            Assert.Equal(1, q.Count);
        }
    }

    private record DoubleItem(double Value);
}
