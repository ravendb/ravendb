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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ExplicitBoostWithFieldOrderBuildsNoScoringState(Options options)
    {
        using var store = GetStoreWithDocumentBoostedIndex(options);
        using var session = store.OpenSession();

        var results = Query(session, "where boost(startsWith(Name, 'maciej'), 2) and Tag = 'a' order by Age as long", out var plan);

        Assert.Equal(new[] { "docs/3", "docs/1", "docs/2" }, results.Select(x => x.Id));
        Assert.DoesNotContain("True", IsBoostingValues(plan));
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData("where Tag = 'a' and not Name = 'Maciej' order by score()", 2, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData("where Tag = 'a' and not boost(Name = 'Maciej', 2) order by score()", 2, SearchEngineMode = RavenSearchEngineMode.Corax)]
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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void VectorSearchWithFieldOrderBuildsNoScoringState()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            for (var i = 0; i < 5; i++)
                session.Store(new VectorDoc { Text = "car", Counter0 = 1, Counter1 = 3 });
            session.Store(new VectorDoc { Text = "cars", Counter0 = 2, Counter1 = 3 });
            session.Store(new VectorDoc { Text = "bike", Counter0 = 1, Counter1 = 2 });
            session.SaveChanges();
        }

        new VectorDocsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            session.Advanced
                .RawQuery<VectorDoc>($"from index '{new VectorDocsIndex().IndexName}' where Counter0 = 1 and Counter1 <= 3 and vector.search(Text, 'car') order by Counter0 include timings()")
                .Timings(out QueryTimings timings)
                .ToList();

            var plan = (QueryInspectionNode)timings.QueryPlan;
            Assert.DoesNotContain("True", IsBoostingValues(plan));
            Assert.NotNull(Find(plan, "MultiUnaryMatch"));
        }
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

    private class VectorDocsIndex : AbstractIndexCreationTask<VectorDoc>
    {
        public VectorDocsIndex()
        {
            Map = docs => from doc in docs
                select new { Text = CreateVector(doc.Text), doc.Counter0, doc.Counter1 };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class VectorDoc
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public int Counter0 { get; set; }
        public int Counter1 { get; set; }
    }
}
