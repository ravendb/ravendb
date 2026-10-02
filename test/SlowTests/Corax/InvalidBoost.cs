using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class InvalidBoost(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData(1e39)]
    [InlineData(-1)]
    [InlineData("-0.5")]
    public void QueryBoostIsRejected(object boost)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" }, "items/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var query = session.Advanced
                .RawQuery<Item>("from Items where boost(Name = 'a', $boost) order by score()")
                .AddParameter("boost", boost);

            Assert.Throws<InvalidQueryException>(() => query.ToList());
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public void DocumentBoostIsRejected()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" }, "items/1");
            session.Store(new Item { Name = "nan" }, "items/2");
            session.Store(new Item { Name = "inf" }, "items/3");
            session.SaveChanges();
        }

        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "BoostedItems",
            Maps = { "from item in docs.Items select new { item.Name }.Boost(item.Name == \"nan\" ? float.NaN : item.Name == \"inf\" ? float.PositiveInfinity : 2f)" }
        }));
        Indexes.WaitForIndexing(store, allowErrors: true);

        var errors = store.Maintenance.Send(new GetIndexErrorsOperation(["BoostedItems"])).Single().Errors;
        Assert.Equal(["items/2", "items/3"], errors.Select(x => x.Document).Order());
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
