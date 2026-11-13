using System;

namespace Corax.Querying.Matches.Meta;

public static class QueryMatch
{
    public const long Invalid = -1;
    public const long Start = 0;
}

public interface IQueryMatch
{
    long Count { get; }

    bool IsBoosting { get; }

    // Guarantees: The output of Fill will be sorted and deduplicated for the call.
    //             Different calls to Fill may return identical values are not guaranteed to be sorted between calls.
    //             0 return means no more matches. 
    int Fill(Span<long> matches);

    // Adds the (boosted) relevance of each present entry into scores[i], positionally aligned to matches[i]; a no-op
    // for unscored sequences. Used when the caller's match order is significant and must be preserved (e.g. a vector
    // post-filter feeding similarity-score order, or any Fill batch that wasn't materialized+sorted). Bitmap-backed
    // leaves implement this as a linear point lookup (one Contains per element) BY DESIGN, not as a perf shortcut:
    // because scores[i] is positionally tied to the unsorted matches[i], the grouped container merge that ScoreSorted
    // uses cannot apply, and on a finalized bitmap each Contains is an O(1) probe. A sort-into-scratch + grouped-merge
    // + scatter-back alternative was benchmarked and ran 7-23x SLOWER (the sort plus random scatter-writes into
    // scores[] cost more than the cheap, branch-predictable point lookups) - see ayende/ravendb#4894. Callers that
    // already hold sorted+deduped matches call ScoreSorted instead.
    void Score(Span<long> matches, Span<float> scores, float boostFactor);

    // Same contract/result as Score, but the caller GUARANTEES `matches` is sorted ascending and deduplicated
    // (holds on the in-memory-score-sort path off the bitmap iterator; vector/post-filter paths keep calling
    // Score). Bitmap-backed leaves exploit the ordering; everyone else delegates to Score.
    void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor);

    QueryInspectionNode Inspect();

    string DebugView => Inspect().ToString();
}

/// <summary>
/// Implemented by query matches backed by a RoaringBitmap, enabling SortingMatch
/// to walk the CompactTree index and intersect batches via AndWith, stopping early
/// when the LIMIT is reached — no full materialization needed.
/// </summary>
public interface IBitmapQueryMatch : IQueryMatch
{
    long MinEntryId { get; }
    long MaxEntryId { get; }

    /// <summary>
    /// Returns a reference to the underlying bitmap data. The caller MUST NOT dispose it.
    /// Used by downstream consumers (vector search filter, faceted lookups) to skip re-materialization.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnscopedRef]
    ref Voron.Data.RoaringBitmaps.RoaringBitmap BitmapState { get; }
}

/// <summary>
/// Implemented by per-entry post-filter match families (spatial / vector). The flag is NOT intrinsic to the
/// type: the same match is a top-level post-filter when the planner lifts it out of an AND, but a pipeline leaf
/// inside an OR branch. <c>QueryPlanBuilder.ApplyPostFilters</c> sets it on the matches it wraps, so inspection
/// reads the recorded role rather than re-deriving from the type.
/// </summary>
public interface IPostFilterMatch : IQueryMatch
{
    bool IsPostFilter { get; set; }

    // Filters an already-materialized batch in place: keeps only the entries in buffer[0..matches) that also
    // satisfy this post-filter predicate, returning the survivor count. Accepts sorted input, returns sorted.
    // Driven by PostFilterMatch.ApplyPostFilters after the inner match's Fill builds the candidate batch.
    int AndWith(Span<long> buffer, int matches);
}
