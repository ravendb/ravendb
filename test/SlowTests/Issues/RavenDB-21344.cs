using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21344 : RavenTestBase
{
    public RavenDB_21344(ITestOutputHelper output) : base(output)
    {
    }
    
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void WildcardSearchQueriesAreNotUsingAnalyzers(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var s = store.OpenSession())
        {
            s.Store(new Item("snapping-dragon"));
            s.SaveChanges();
        }
        
        using (var s = store.OpenSession())
        {
            // we do NOT analyze wildcard values, so we are basically doing contains "snapping-turtle"
            // on the _analyzed_ values in the index, and there should be none there 
            //
            // https://ayende.com/blog/191841-B/understanding-query-processing-and-wildcards-in-ravendb
            var l = s.Query<Item>()
                .Search(x => x.Name, "*snapping-turtle*")
                .ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Empty(l);
        }
    }
    
      
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StartsSearchQueriesShouldHandleDifferentCasing(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var s = store.OpenSession())
        {
            s.Store(new Item("snapping-dragon"));
            s.SaveChanges();
        }
        
        using (var s = store.OpenSession())
        {
            // we do NOT analyze wildcard values, so we are basically doing contains "snapping-turtle"
            // on the _analyzed_ values in the index, and there should be none there 
            //
            // https://ayende.com/blog/191841-B/understanding-query-processing-and-wildcards-in-ravendb
            var l = s.Query<Item>()
                .Search(x => x.Name, "SNAP*")
                .ToList();
            Assert.NotEmpty(l);
        }
        
        using (var s = store.OpenSession())
        {
            // we do NOT analyze wildcard values, so we are basically doing contains "snapping-turtle"
            // on the _analyzed_ values in the index, and there should be none there 
            //
            // https://ayende.com/blog/191841-B/understanding-query-processing-and-wildcards-in-ravendb
            var l = s.Query<Item>()
                .Search(x => x.Name, "*DRagoN*")
                .ToList();
            Assert.NotEmpty(l);
        }
    }

    private record Item(string Name);
}
