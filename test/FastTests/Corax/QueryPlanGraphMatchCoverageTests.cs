using System;
using System.Collections.Generic;
using System.Linq;
using Corax.Querying.Matches.Meta;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Tripwire for the query-plan graph (<c>QueryPlanGraph</c>). The graph renders a compiled query as a
/// dataflow diagram by switching on the <em>kind</em> of every <see cref="IQueryMatch"/> that can appear in a
/// plan: bitmap-pipeline leaves, slot producers (direct/compound scans), result wrappers (sort/boost), and the
/// per-entry POST-FILTERS (spatial / vector). The post-filter recognition in particular is string-based
/// (<c>operation.Contains("Spatial") || operation.Contains("VectorSearch")</c>), so a newly-added match type
/// that does not fit one of these buckets — or a new post-filter whose name does not contain those tokens —
/// would be silently dropped from the graph with no compile-time signal.
///
/// This test reflects over every concrete <see cref="IQueryMatch"/> implementation in the Corax assembly and
/// asserts each is accounted for in the maintained classification below. Adding a new match type breaks this
/// test until the author classifies it here AND confirms <c>QueryPlanGraph</c> renders it (the failure message
/// says exactly that). Post-filter matches additionally must satisfy the graph's Contains-based predicate, so a
/// post-filter named without "Spatial"/"VectorSearch" is caught here rather than vanishing from the diagram.
/// </summary>
public class QueryPlanGraphMatchCoverageTests : RavenTestBase
{
    public QueryPlanGraphMatchCoverageTests(ITestOutputHelper output) : base(output)
    {
    }

    // Per-entry post-filters: rendered as the green PostFilter chain that consumes the candidate bitmap.
    // QueryPlanGraph.IsPostFilterOp matches these by name Contains("Spatial") || Contains("VectorSearch"),
    // mirrored by PostFilterPredicate below — every name here MUST satisfy that predicate.
    private static readonly HashSet<string> PostFilterMatches = new()
    {
        "SpatialMatch",
        "VectorSearchMatch",
        "MultiVectorSearchMatch",
    };

    // Result-shaping wrappers that sit above the bitmap pipeline (QueryPlanGraph.ResultWrapperOps): peeled off
    // and rendered as the dataflow tail (sort strategy / boost factor → Result).
    private static readonly HashSet<string> ResultWrapperMatches = new()
    {
        "SortingMatch",
        "SortingMultiMatch",
        "BoostingMatch",
    };

    // Slot producers / streaming-scan drivers: surfaced as the producer node feeding the pipeline.
    private static readonly HashSet<string> ProducerMatches = new()
    {
        "DirectScanSimpleMatch",
        "DirectScanFilteredMatch",
        "SortedDrivingMatch",
        "SortedDrivingWithTieBreakMatch",
    };

    // Bitmap-pipeline leaves / accumulators: rendered as op-template nodes writing their DestSlot.
    private static readonly HashSet<string> PipelineMatches = new()
    {
        "TermMatch",
        "TermsProviderMatch",
        "BitmapMatch",
        "LazyOrMatch",
        "PhraseMatch",
        "AllEntriesMatch",
    };

    // Plan roots: CompiledQueryMatch is the bitmap pipeline root; PostFilterMatch is the all-entries bypass root;
    // EmptyQueryMatch is the degenerate empty-result plan (missing field/term) returned directly to the caller.
    // ToGraphviz receives it at the top level, finds no CompiledQuery/PostFilterMatch, and renders the valid
    // "no compiled op stream" fallback — it carries no DestSlot ops, so there is nothing else to draw.
    // NegatedPostFilterMatch is the root wrapper for negated spatial/vector clauses: it materializes the
    // candidate universe and subtracts each clause's matches. ToGraphviz recurses through it (FindNode) to render
    // the inner CompiledQuery pipeline; surfacing its subtracted post-filters in the graph is a separate follow-up.
    private static readonly HashSet<string> RootMatches = new()
    {
        "CompiledQueryMatch",
        "PostFilterMatch",
        "EmptyQueryMatch",
        "NegatedPostFilterMatch",
    };

    private static bool PostFilterPredicate(string name)
        => name.Contains("Spatial") || name.Contains("VectorSearch");

    [RavenFact(RavenTestCategory.Corax)]
    public void EveryQueryMatchTypeIsClassifiedForThePlanGraph()
    {
        HashSet<string> classified = new();
        classified.UnionWith(PostFilterMatches);
        classified.UnionWith(ResultWrapperMatches);
        classified.UnionWith(ProducerMatches);
        classified.UnionWith(PipelineMatches);
        classified.UnionWith(RootMatches);

        List<string> discovered = typeof(IQueryMatch).Assembly
            .GetTypes()
            .Where(t => t.IsAbstract == false
                        && t.IsInterface == false
                        && typeof(IQueryMatch).IsAssignableFrom(t))
            .Select(SimpleName)
            .Distinct()
            .ToList();

        List<string> unclassified = discovered.Where(name => classified.Contains(name) == false).OrderBy(n => n).ToList();
        Assert.True(unclassified.Count == 0,
            "New IQueryMatch type(s) not classified for QueryPlanGraph: " + string.Join(", ", unclassified) + ". " +
            "Add each to the appropriate set in this test AND make sure QueryPlanGraph renders it " +
            "(IsPostFilterOp for spatial/vector post-filters, ResultWrapperOps for sort/boost wrappers, " +
            "a producer node for scans, or the op-template for pipeline leaves).");

        // The classification must not rot: a removed/renamed match type leaves a stale entry here.
        List<string> stale = classified.Where(name => discovered.Contains(name) == false).OrderBy(n => n).ToList();
        Assert.True(stale.Count == 0,
            "Classification lists a match type that no longer exists in Corax: " + string.Join(", ", stale) + ". Remove the stale entry.");
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void EveryPostFilterMatchIsRecognizedByTheGraphsNamePredicate()
    {
        // The graph recognises post-filters by name token, not type identity. Any match we declare a post-filter
        // must satisfy that predicate, otherwise QueryPlanGraph would drop it from the post-filter chain.
        List<string> notRecognized = PostFilterMatches.Where(name => PostFilterPredicate(name) == false).OrderBy(n => n).ToList();
        Assert.True(notRecognized.Count == 0,
            "Post-filter match type(s) whose name is not matched by QueryPlanGraph.IsPostFilterOp " +
            "(Contains \"Spatial\"/\"VectorSearch\"): " + string.Join(", ", notRecognized) + ". " +
            "Either rename to include the token or broaden IsPostFilterOp.");
    }

    private static string SimpleName(Type type)
    {
        string name = type.Name;
        int tick = name.IndexOf('`');
        return tick < 0 ? name : name.Substring(0, tick);
    }
}
