using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class ScoringOnlyWhenScoresAreConsumed(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void FacetsWithWhereOnDocumentBoostedIndex(Options options)
    {
        using var store = GetStoreWithDocumentBoostedIndex(options);
        using var session = store.OpenSession();

        var facets = session.Query<Doc, DocsIndex>()
            .Where(x => x.Tag == "a")
            .AggregateBy(builder => builder.ByField(x => x.Name))
            .Execute();

        var counts = facets[nameof(Doc.Name)].Values.ToDictionary(x => x.Range, x => x.Count);
        Assert.Equal(new Dictionary<string, int> { ["maciej"] = 1, ["maciej2"] = 2 }, counts);
    }

    private IDocumentStore GetStoreWithDocumentBoostedIndex(Options options)
    {
        var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Name = "Maciej", Tag = "a" }, "docs/1");
            session.Store(new Doc { Name = "Maciej2", Tag = "a" }, "docs/2");
            session.Store(new Doc { Name = "Maciej2", Tag = "a" }, "docs/3");
            session.Store(new Doc { Name = "Random", Tag = "b" }, "docs/4");
            session.SaveChanges();
        }

        new DocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class DocsIndex : AbstractIndexCreationTask<Doc>
    {
        public DocsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Name, doc.Tag }.Boost(2);
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
    }
}
