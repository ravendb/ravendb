using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21328 : RavenTestBase
{
    public RavenDB_21328(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void QueryingOnMultipleTagsShouldReturnSingleResult(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var s = store.OpenSession())
        {
            s.Store(new Item(new[]{"dogs/1", "dogs/2", "dogs/3"}, "Dogs"));
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            var items = s.Advanced
                .RawQuery<object>("from Items where search(Tags, 'dogs*') order by Name select id(), Name")
                .ToList();
            Assert.Equal(1, items.Count);
        }
    }

    private record Item(string[] Tags, string Name);
}
