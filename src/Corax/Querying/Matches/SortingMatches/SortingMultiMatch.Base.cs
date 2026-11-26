using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Non-generic abstract base for multi-field (ORDER BY ..., ...) sorting matches.
/// Mirrors <see cref="SortingMatch"/> for the multi-comparator case so callers can
/// pattern-match without referencing the <c>TInner</c> type parameter.
/// </summary>
public abstract class SortingMultiMatch : IQueryMatch, IDisposable, IRequireSortingDataTransfer
{
    /// <summary>Total number of matching entries (set after the first Fill call).</summary>
    public long TotalResults;

    /// <summary>True when the candidate batch is sorted ascending by entry id — i.e. it came from the
    /// bitmap-backed materialization (the bitmap iterator yields in order) rather than the non-bitmap drain.
    /// The score comparer uses this to take the sorted-aware <see cref="IQueryMatch.ScoreSorted"/> fast path.</summary>
    internal bool CandidatesAreSorted;

    /// <summary>Ticks spent on sort-specific work (the multi-comparer heap sort). Excludes the inner match's
    /// execution, timed onto the child CompiledQuery node — counting it here too would double-count.
    /// <see cref="SortingMultiMatch{TInner}.Inspect"/> emits this as the sort node's "Ms".</summary>
    public long SortingTimeInTicks;

    public abstract bool IsBoosting { get; }
    public abstract long Count { get; }
    public abstract int Fill(Span<long> buffer);
    // Top-level sort: its own Score is a no-op (never nested in another match's score chain), so ScoreSorted mirrors it.
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}
