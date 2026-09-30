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
        Assert.DoesNotContain("True", IsBoostingValues(plan));

        Query(session, "where Tag = 'a' order by score()", out var scoredPlan);
        Assert.Equal("True", FindTermMatch(scoredPlan).Parameters["IsBoosting"]);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData("where Tag = 'a' and not Name = 'Maciej' order by score()", 2, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Tag = 'b' or not Name = 'Maciej' order by score()", 3, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Name != 'Maciej' order by score()", 3, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Tag = 'a' and not startsWith(Name, 'maciej2') order by score()", 1, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Tag = 'a' and not Age between 25 and 35 order by score()", 2, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ExcludedSideOfNegationIsNotScored(Options options, string rql, int expectedCount)
    {
        using var store = GetStoreWithDocumentBoostedIndex(options);
        using var session = store.OpenSession();

        var results = Query(session, rql, out var plan);

        Assert.Equal(expectedCount, results.Count);
        var andNot = Find(plan, "BinaryMatch [AndNot]");
        Assert.NotNull(andNot);
        Assert.Equal("False", andNot.Children[1].Parameters["IsBoosting"]);

        Query(session, "where Name = 'Maciej' order by score()", out var positivePlan);
        Assert.Equal("True", FindTermMatch(positivePlan).Parameters["IsBoosting"]);
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

    private static bool[] HasIndexScore(IDocumentStore store, string orderBy)
    {
        using var session = store.OpenSession();
        var results = session.Advanced
            .RawQuery<Doc>($"from index '{new DocsIndex().IndexName}' where boost(Tag = 'a', 2) or boost(Name = 'Maciej', 3) {orderBy}")
            .ToList();

        Assert.NotEmpty(results);
        return results.Select(x => session.Advanced.GetMetadataFor(x).ContainsKey(Raven.Client.Constants.Documents.Metadata.IndexScore)).ToArray();
    }

    private static IEnumerable<string> IsBoostingValues(QueryInspectionNode node)
    {
        if (node.Parameters != null && node.Parameters.TryGetValue("IsBoosting", out var isBoosting))
            yield return isBoosting;

        foreach (var child in node.Children ?? new List<QueryInspectionNode>())
        foreach (var value in IsBoostingValues(child))
            yield return value;
    }

    private static QueryInspectionNode FindTermMatch(QueryInspectionNode node) => Find(node, "TermMatch");

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node.Operation.StartsWith(operation))
            return node;

        return (node.Children ?? new List<QueryInspectionNode>()).Select(child => Find(child, operation)).FirstOrDefault(found => found != null);
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
