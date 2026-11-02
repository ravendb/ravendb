using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class ScoringOnlyWhenScoresAreConsumed(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void FieldOrderOnDocumentBoostedIndexBuildsNoScoringState(Options options)
    {
        using var store = GetStoreWithDocumentBoostedIndex(options);
        using var session = store.OpenSession();

        var byAge = Query(session, "where Tag = 'a' order by Age as long", out var plan);

        Assert.Equal(new[] { "docs/3", "docs/1", "docs/2" }, byAge.Select(x => x.Id));
        Assert.DoesNotContain(MatchDispatch, Dispatches(plan));

        Query(session, "where Tag = 'a' order by score()", out var scoredPlan);
        Assert.Contains(MatchDispatch, Dispatches(scoredPlan));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ExplicitBoostWithFieldOrderBuildsNoScoringState(Options options)
    {
        using var store = GetStoreWithDocumentBoostedIndex(options);
        using var session = store.OpenSession();

        var results = Query(session, "where boost(startsWith(Name, 'maciej'), 2) and Tag = 'a' order by Age as long", out var plan);

        Assert.Equal(new[] { "docs/3", "docs/1", "docs/2" }, results.Select(x => x.Id));
        Assert.DoesNotContain(MatchDispatch, Dispatches(plan));
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Facets)]
    public void FacetOnTheSameWhereDoesNotDropImplicitScoreOrdering()
    {
        using var store = GetStoreWithDocumentBoostedIndex(Options.ForSearchEngine(RavenSearchEngineMode.Corax, includeScoresAndDistances: true));
        using var session = store.OpenSession();

        session.Advanced
            .RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where Tag = 'a' select facet(Name)")
            .ExecuteAggregation();

        var results = Query(session, "where Tag = 'a'", out var plan);

        Assert.Equal(3, results.Count);
        Assert.Contains(MatchDispatch, Dispatches(plan));
        Assert.All(results, x => Assert.True(session.Advanced.GetMetadataFor(x).ContainsKey(Raven.Client.Constants.Documents.Metadata.IndexScore)));
    }

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

    private const string MatchDispatch = "Match";

    private static List<Doc> Query(IDocumentSession session, string rql, out QueryInspectionNode plan)
    {
        var results = session.Advanced
            .RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' {rql} include timings()")
            .Timings(out QueryTimings timings)
            .ToList();

        plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        return results;
    }

    private static IEnumerable<string> Dispatches(QueryInspectionNode node)
    {
        if (node.Parameters != null && node.Parameters.TryGetValue("Dispatch", out var dispatch))
            yield return dispatch;

        foreach (var child in node.Children ?? [])
        foreach (var value in Dispatches(child))
            yield return value;
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
