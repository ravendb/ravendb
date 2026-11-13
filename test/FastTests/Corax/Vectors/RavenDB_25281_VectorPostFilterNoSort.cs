using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class RavenDB_25281_VectorPostFilterNoSort(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class CityVecDoc
    {
        public string Id { get; set; }
        public string City { get; set; }
        public float[] Embedding { get; set; }
    }

    private sealed class CityVecIndex : AbstractIndexCreationTask<CityVecDoc>
    {
        public CityVecIndex()
        {
            Map = docs => from d in docs
                          select new
                          {
                              d.City,
                              Embedding = CreateVector(d.Embedding),
                          };
        }
    }

    // Query vector points east ([1,0]). The seeded docs lie on a gradient of decreasing cosine similarity to it.
    private static readonly float[] Query = [1f, 0f];

    private static void Seed(IDocumentStore store)
    {
        using var session = store.OpenSession();
        // NYC docs on a clearly-separated similarity gradient against [1,0] (all unit vectors): sim 1.0 > 0.8 > 0.6.
        session.Store(new CityVecDoc { Id = "docs/nyc-1", City = "NYC", Embedding = [1.0f, 0.0f] });
        session.Store(new CityVecDoc { Id = "docs/nyc-2", City = "NYC", Embedding = [0.8f, 0.6f] });
        session.Store(new CityVecDoc { Id = "docs/nyc-3", City = "NYC", Embedding = [0.6f, 0.8f] });
        // A different city with the MOST similar embedding of all — must be filtered out by the City clause,
        // proving the vector runs as a genuine post-filter and not a global nearest-neighbour scan.
        session.Store(new CityVecDoc { Id = "docs/par-1", City = "Paris", Embedding = [1.0f, 0.0f] });
        session.SaveChanges();
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

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void AndFilteredVectorSearch_NoOrderBy_SkipsSortWrapper_AndStreamsScoreOrder()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var ids = session.Advanced
            .RawQuery<CityVecDoc>(
                @"from index 'CityVecIndex'
                  where City = $c and vector.search(Embedding, $vec, $minSim)
                  include timings()")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .AddParameter("minSim", 0.5f)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .ToList();

        // Paris is excluded by the City post-filter even though it is the single most-similar doc.
        Assert.Equal(new[] { "docs/nyc-1", "docs/nyc-2", "docs/nyc-3" }, ids);

        // The single vector post-filter already streams its HNSW output in similarity-score order, so the
        // implicit score SortingMatch wrapper must NOT be added: the plan root is the CompiledQuery, not a sort.
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.Null(FindNode(plan, "SortingMatch"));
        Assert.Null(FindNode(plan, "SortingMultiMatch"));
        // The vector search is still present, as a post-filter hanging off the bitmap pipeline.
        Assert.NotNull(FindNode(plan, "VectorSearchMatch"));
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void AndFilteredVectorSearch_ExplicitOrderByScore_SkipsSortWrapper()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var ids = session.Advanced
            .RawQuery<CityVecDoc>(
                @"from index 'CityVecIndex'
                  where City = $c and vector.search(Embedding, $vec, $minSim)
                  order by score()
                  include timings()")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .AddParameter("minSim", 0.5f)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .ToList();

        Assert.Equal(new[] { "docs/nyc-1", "docs/nyc-2", "docs/nyc-3" }, ids);

        // `ORDER BY score()` asks for exactly the order the vector emits natively — the wrapper is still redundant.
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.Null(FindNode(plan, "SortingMatch"));
        Assert.Null(FindNode(plan, "SortingMultiMatch"));
        Assert.NotNull(FindNode(plan, "VectorSearchMatch"));
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrBranchVectorSearch_KeepsSortWrapper()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new CityVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        session.Advanced
            .RawQuery<CityVecDoc>(
                @"from index 'CityVecIndex'
                  where vector.search(Embedding, $vec, $minSim) or City = $c
                  include timings()")
            .AddParameter("c", "Paris")
            .AddParameter("vec", Query)
            .AddParameter("minSim", 0.5f)
            .Timings(out var timings)
            .WaitForNonStaleResults()
            .ToList();

        // Inside an OR branch the vector is an ordinary pipeline leaf, not a post-filter, so it does NOT provide
        // the result order on its own — the implicit score SortingMatch wrapper must be retained. The plan root is
        // therefore the sort wrapper, with the CompiledQuery bitmap pipeline as a descendant.
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.NotEqual("CompiledQuery", plan.Operation);
        Assert.NotNull(FindNode(plan, "SortingMatch"));
        Assert.NotNull(FindNode(plan, "CompiledQuery"));
    }
}
