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
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void IndexScoreIsReturnedOnlyWhenSortingByScore()
    {
        using var store = GetStoreWithDocumentBoostedIndex(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));

        Assert.All(HasIndexScore(store, "order by Age as long"), Assert.False);
        Assert.All(HasIndexScore(store, "order by Age as long, score()"), Assert.True);
    }

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
            session.Store(new Doc { Name = "Maciej", Tag = "a", Age = 30 }, "docs/1");
            session.Store(new Doc { Name = "Maciej2", Tag = "a", Age = 40 }, "docs/2");
            session.Store(new Doc { Name = "Maciej2", Tag = "a", Age = 20 }, "docs/3");
            session.Store(new Doc { Name = "Random", Tag = "b", Age = 50 }, "docs/4");
            session.SaveChanges();
        }

        new DocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private static bool[] HasIndexScore(IDocumentStore store, string orderBy)
    {
        using var session = store.OpenSession();
        var results = session.Advanced
            .RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where boost(Tag = 'a', 2) or boost(Name = 'Maciej', 3) {orderBy}")
            .ToList();

        Assert.NotEmpty(results);
        return results.Select(x => session.Advanced.GetMetadataFor(x).ContainsKey(Raven.Client.Constants.Documents.Metadata.IndexScore)).ToArray();
    }

    private class DocsIndex : AbstractIndexCreationTask<Doc>
    {
        public DocsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Name, doc.Tag, doc.Age }.Boost(2);
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
        public int Age { get; set; }
    }
}
