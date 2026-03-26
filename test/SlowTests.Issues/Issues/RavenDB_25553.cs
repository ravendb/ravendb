using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25553(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DynamicIndexExactInQueryWillUseExactField(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "Tarzan" });
            session.Store(new Item { Name = "tarzan" });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.DocumentQuery<Item>()
                .WaitForNonStaleResults()
                .WhereIn(x => x.Name, new[] { "Tarzan" }, exact: true)
                .SingleOrDefault();
            Assert.NotNull(results);            
        }
    }
    
    private class Item
    {
        public string Name { get; set; }
    }
}
