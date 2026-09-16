using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>The concrete strategy a <see cref="SortingMatch"/> used to produce the sorted result set. </summary>
public enum CoraxSortingStrategy : byte
{
    RandomOrder,
    /// <summary>Materialize the whole candidate set and heap-sort it.</summary>
    InMemorySort,
    /// <summary>Walk the sort index in order, intersecting each batch against the candidate bitmap.</summary>
    IndexOrderStreaming,
    /// <summary>Started with IndexOrderStreaming, didn't get enough results and bailed to InMemorySort</summary>
    IndexOrderFallbackToInMemorySort,
}

public enum SortStrategyDecision : byte
{
    NotEvaluated,

    /// <summary>The sort axis has no in-order index to walk (computed score(), spatial distance, alphanumeric, or a
    /// field some documents lack), so IndexOrderStreaming is structurally impossible and the gate is skipped — always
    /// InMemorySort. Surfaced so `order by score()` etc. shows WHY it can't stream instead of an empty reason.</summary>
    NotIterableSortField,

    /// <summary>No usable LIMIT (take &lt; 0, or take &gt;= candidates): streaming can't terminate early, so it would walk the whole index. Chose InMemorySort.</summary>
    NoLimitFullScan,

    /// <summary>Estimated streamed entries &lt; candidates x cost ratio: streaming is the cheaper plan. Chose IndexOrderStreaming.</summary>
    StreamCheaper,

    /// <summary>Estimated streamed entries &gt;= candidates x cost ratio: the index walk would read more (cost-weighted) than
    /// materialize-and-sort. Chose InMemorySort.</summary>
    SortCheaper,
}

public abstract class SortingMatch : IQueryMatch, IDisposable, IRequireSortingDataTransfer
{
    public const int SortBatchSize = 8192;

    /// <summary>
    /// Per-thread byte buffer for UTF-8 encode in SliceEqualsUtf8, in non generic base class.
    /// </summary>
    [ThreadStatic]
    internal static byte[] Utf8ThreadBuffer;

    public long TotalResults;

    public long SortingTimeInTicks;

    public CoraxSortingStrategy? SortStrategy;

    /// <summary>
    /// True when the results are already sorted by entry IDs. 
    /// </summary>
    internal bool CandidatesAreSorted;

    /// <summary>Sort strategy pinned by the reserved <c>$rvn_corax_sort</c> parameter. Honored only for the InMemorySort vs IndexOrderStreaming choice
    /// on an iterable sort index; forcing IndexOrderStreaming also suppresses the over-scan bailout.</summary>
    public CoraxSortingStrategy? ForcedStrategy;

    /// <summary>Streaming strategy only: entry IDs read from the sort index and intersected against the candidate set.</summary>
    public long EntriesStreamed;

    // use for telemetry / inspection
    public SortStrategyDecision GateDecision;
    public double StreamScanEstimateRaw;
    public double StreamScanEstimateInflated;
    public double StreamScanInflationFactor;
    public double GateThreshold;

    public abstract bool IsBoosting { get; }
    public abstract long Count { get; }
    public abstract int Fill(Span<long> buffer);

    // A SortingMatch is never nested inside another match's score chain, nothing to do here
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);
    
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}

public interface IRequireSortingDataTransfer
{
    void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
}
