using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26959(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class Movie
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public float[] OverviewVector { get; set; }
    }

    private sealed class Movies_Semantic : AbstractIndexCreationTask<Movie>
    {
        public Movies_Semantic()
        {
            Map = movies => from m in movies
                            select new
                            {
                                m.Title,
                                OverviewVector = CreateVector(m.OverviewVector)
                            };

            Index("OverviewVector", FieldIndexing.Search);
            Configuration = new IndexConfiguration
            {
                ["Indexing.Corax.IncludeDocumentScore"] = "true"
            };
        }
    }

    // The query points along [1,0]; the three docs lie on a decreasing cosine-similarity gradient against it.
    // Since this index stores cosine distance and score = 1 - distance, their relevance scores are
    // 1.0 > 0.8 > 0.6 - "Closest" is the most relevant (highest score), "Furthest" the least.
    private static readonly float[] TargetVector = [1f, 0f];

    private void Seed(IDocumentStore store)
    {
        new Movies_Semantic().Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new Movie { Title = "Middle", OverviewVector = [0.8f, 0.6f] });
            session.Store(new Movie { Title = "Furthest", OverviewVector = [0.6f, 0.8f] });
            session.Store(new Movie { Title = "Closest", OverviewVector = [1.0f, 0.0f] });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
    }

    private static string[] QueryTitles(IDocumentSession session, string direction)
    {
        return session.Advanced.RawQuery<Movie>($@"
                from index 'Movies/Semantic'
                where vector.search(OverviewVector, $targetVector, 0.0, 20)
                order by score() {direction}")
            .AddParameter("targetVector", TargetVector)
            .WaitForNonStaleResults()
            .ToList()
            .Select(m => m.Title)
            .ToArray();
    }

    // RavenDB's score ordering treats `order by score()` and `order by score() asc` as "highest score first"
    // (most relevant), and `order by score() desc` as "lowest score first". These three tests assert that vector
    // search honours that convention rather than emitting its native nearest-first order regardless of direction.

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScore_Default_ReturnsHighestScoreFirst()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);

        using var session = store.OpenSession();
        Assert.Equal(new[] { "Closest", "Middle", "Furthest" }, QueryTitles(session, direction: ""));
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScore_Asc_ReturnsHighestScoreFirst()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);

        using var session = store.OpenSession();
        Assert.Equal(new[] { "Closest", "Middle", "Furthest" }, QueryTitles(session, direction: "asc"));
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScore_Desc_ReturnsLowestScoreFirst()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);

        using var session = store.OpenSession();
        Assert.Equal(new[] { "Furthest", "Middle", "Closest" }, QueryTitles(session, direction: "desc"));
    }

    // Streaming path: `order by score()` / `asc` ask for exactly the order the vector match emits natively
    // (nearest-first == highest-score-first), so the plan streams it directly with no sorting wrapper.
    // `order by score() desc` reverses that order, so the plan MUST fall through to an explicit sort.

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScore_Ascending_StreamsWithoutSortWrapper()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Movie>(@"
                from index 'Movies/Semantic'
                where vector.search(OverviewVector, $targetVector, 0.0, 20)
                order by score()
                include timings()")
            .AddParameter("targetVector", TargetVector)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(new[] { "Closest", "Middle", "Furthest" }, results.Select(m => m.Title).ToArray());

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.Null(FindNode(plan, "SortingMatch"));
        Assert.Null(FindNode(plan, "SortingMultiMatch"));
        Assert.NotNull(FindNode(plan, "VectorSearchMatch"));
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScore_Descending_UsesExplicitSort()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Movie>(@"
                from index 'Movies/Semantic'
                where vector.search(OverviewVector, $targetVector, 0.0, 20)
                order by score() desc
                include timings()")
            .AddParameter("targetVector", TargetVector)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList();

        Assert.Equal(new[] { "Furthest", "Middle", "Closest" }, results.Select(m => m.Title).ToArray());

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.NotNull(FindNode(plan, "SortingMatch"));
    }

    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node is null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children is null)
            return null;
        foreach (var child in node.Children)
        {
            var hit = FindNode(child, operation);
            if (hit != null)
                return hit;
        }

        return null;
    }
}
