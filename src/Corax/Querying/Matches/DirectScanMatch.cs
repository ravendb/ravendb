using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.RoaringBitmaps;
using Corax.Utils;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Walks a driving tree in sort order, optionally checking residual predicates per entry
/// via stored field reads. Two subclasses handle the residual/no-residual cases:
/// <see cref="DirectScanSimpleMatch"/> (simple pass-through) and
/// <see cref="DirectScanFilteredMatch"/> (evaluates compiled predicate delegate).
/// </summary>
public abstract class DirectScanMatchBase : IQueryMatch, IDisposable
{
    protected readonly IndexSearcher Searcher;
    protected readonly LowLevelTransaction Llt;
    protected readonly IQueryMatch DrivingMatch;
    protected readonly int Take;
    protected long TotalMatched;

    protected RoaringBitmap EmittedBitmap;

    protected long TreeEntriesScanned;
    protected long EntriesPassedFilter;
    protected long EntriesRejected;
    protected long TreeScanTicks;
    protected long EntryScanTicks;
    protected string StoppedReason;

    public string DrivingTreeName;
    public string DrivingClause;
    public string SeekBound;
    public string Direction;
    public string ResidualDescription;
    public string Reason;

    /// <summary>
    /// The driving provider's posting count (O(distinct terms)), or -1 when it cannot be derived cheaply. 
    /// </summary>
    public long KnownExactTotal = -1;

    /// <summary>
    /// Cost in Stopwatch ticks & the number of scanned terms to compute <see cref="KnownExactTotal"/>, for the introspection graph. 
    /// </summary>
    public long KnownTotalProbeTicks = -1;
    public int KnownTotalProbeTerms;

    protected DirectScanMatchBase(IndexSearcher searcher, IQueryMatch drivingMatch, int take)
    {
        Searcher = searcher;
        Llt = searcher.Transaction.LowLevelTransaction;
        DrivingMatch = drivingMatch;
        Take = take;
        ByteStringContext allocator = searcher.Allocator;
        EmittedBitmap = new RoaringBitmap(allocator);
    }

    public long Count => TotalMatched;
    public bool IsBoosting => false;

    public abstract int Fill(Span<long> matches);

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) { }


    public virtual QueryInspectionNode Inspect()
    {
        double tickFreq = Stopwatch.Frequency / 1000.0;
        var parameters = new Dictionary<string, string>();

        if (DrivingTreeName != null) parameters["DrivingTree"] = DrivingTreeName;
        if (DrivingClause != null) parameters["DrivingClause"] = DrivingClause;
        if (SeekBound != null) parameters["SeekBound"] = SeekBound;
        if (Direction != null) parameters["TreeDirection"] = Direction;
        if (ResidualDescription != null) parameters["ResidualPredicates"] = ResidualDescription;
        if (Reason != null) parameters["Reason"] = Reason;

        if (TreeScanTicks > 0) parameters["TreeScan_ms"] = (TreeScanTicks / tickFreq).ToString("F3");
        if (EntryScanTicks > 0) parameters["EntryScans_ms"] = (EntryScanTicks / tickFreq).ToString("F3");

        parameters["TreeEntriesScanned"] = TreeEntriesScanned.ToString("N0");
        parameters["Output"] = TotalMatched.ToString("N0");
        if (StoppedReason != null) parameters["StoppedAt"] = StoppedReason;
        if (KnownExactTotal >= 0) parameters["KnownExactTotal"] = KnownExactTotal.ToString("N0");
        if (KnownTotalProbeTicks >= 0)
        {
            parameters["KnownTotalProbe_ms"] = (KnownTotalProbeTicks / tickFreq).ToString("F3");
            parameters["KnownTotalProbeTerms"] = KnownTotalProbeTerms.ToString("N0");
        }
        return new QueryInspectionNode("DirectScan", parameters: parameters);
    }

    public void Dispose()
    {
        EmittedBitmap.Dispose();
        (DrivingMatch as IDisposable)?.Dispose();
    }
}

/// <summary>DirectScan with no residual predicates — simple dedup + pass-through.</summary>
public sealed class DirectScanSimpleMatch(IndexSearcher searcher, IQueryMatch drivingMatch, int take) : DirectScanMatchBase(searcher, drivingMatch, take)
{
    [SkipLocalsInit]
    public override unsafe int Fill(Span<long> matches)
    {
        if (Take > 0 && TotalMatched >= Take)
            return 0;

        int count = 0;
        int remaining = Take > 0 ? (int)Math.Min(matches.Length, Take - TotalMatched) : matches.Length;
        Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        while (count < remaining)
        {
            int batchSize = Math.Min(QueryPrimitives.EntryScanBatchSize, remaining - count);
            if (batchSize == 0)
                break;

            long t0 = Stopwatch.GetTimestamp();
            int read = DrivingMatch.Fill(batch[..batchSize]);
            TreeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0)
            {
                StoppedReason ??= "TreeExhausted";
                break;
            }
            TreeEntriesScanned += read;

            // Dedup the whole batch against EmittedBitmap in one bulk pass
            int kept = EmittedBitmap.DedupAddNew(batch, read);
            batch[..kept].CopyTo(matches[count..]);
            count += kept;
        }

        if (Take > 0 && TotalMatched + count >= Take)
            StoppedReason ??= "ReachedQueryLimits";

        TotalMatched += count;
        return count;
    }
}

/// <summary>
/// DirectScan with residual predicates: evaluates a compiled IL delegate
/// against stored-field readers for each entry batch. 
/// </summary>
public sealed class DirectScanFilteredMatch(
    IndexSearcher searcher,
    IQueryMatch drivingMatch,
    QueryExecution exec,
    int take,
    ResidualScanIlEmitter.ResidualScanPredicate precompiledDelegate)
    : DirectScanMatchBase(searcher, drivingMatch, take)
{
    /// <summary>Per-execution state — the emitted IL loads analyzer-encoded slices,
    /// field-root pages, and direct long/double values from this object via baked field indices.</summary>
    private readonly QueryExecution _exec = exec;

    public override QueryInspectionNode Inspect()
    {
        var result = base.Inspect();
        var parameters = result.Parameters;
        parameters["EntriesPassedFilter"] = EntriesPassedFilter.ToString("N0");
        parameters["EntriesRejected"] = EntriesRejected.ToString("N0");
        return result;
    }

    [SkipLocalsInit]
    public override unsafe int Fill(Span<long> matches)
    {
        if (Take > 0 && TotalMatched >= Take)
            return 0;

        int count = 0;
        int remaining = Take > 0 ? (int)Math.Min(matches.Length, Take - TotalMatched) : matches.Length;
        int batchSize = Math.Min(QueryPrimitives.EntryScanBatchSize, Math.Max(1, remaining));
        Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<int> indices = stackalloc int[QueryPrimitives.EntryScanBatchSize];
        Span<bool> passed = stackalloc bool[QueryPrimitives.EntryScanBatchSize];
        Span<long> sortedIds = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<long> containerLocs = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<UnmanagedSpan> containerSpans = stackalloc UnmanagedSpan[QueryPrimitives.EntryScanBatchSize];
        Span<long> packedIds = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<int> packedOrigIdx = stackalloc int[QueryPrimitives.EntryScanBatchSize];
        var readersArr = ArrayPool<EntryTermsReader>.Shared.Rent(QueryPrimitives.EntryScanBatchSize);
        // The compiled predicate evaluates readers one at a time and RunEntryScan-style consumers
        // only read entry IDs afterward, so the whole batch can share a single scratch key.
        var scanKey = Llt.AcquireCompactKey();
        try
        {
            Searcher.InitializeSpecialTermsMarkers();
            
            while (count < remaining)
            {
                long t0 = Stopwatch.GetTimestamp();
                int read = DrivingMatch.Fill(batch[..batchSize]);
                TreeScanTicks += Stopwatch.GetTimestamp() - t0;

                if (read == 0)
                {
                    StoppedReason ??= "TreeExhausted";
                    break;
                }

                TreeEntriesScanned += read;

                var sorted = sortedIds[..read];
                batch[..read].CopyTo(sorted);
                RoaringBitmap.InitializeIndices(indices, read);
                sorted.Sort(indices[..read]);

                passed[..read].Clear();

                long t1 = Stopwatch.GetTimestamp();

                var locs = containerLocs[..read];
                Searcher.ResolveEntryLocations(sorted, locs);

                var spans = containerSpans[..read];
                Container.GetAllSortedByPage(Llt, locs, spans, Llt.PageLocator);
                
                int packed = 0;
                for (int s = 0; s < read; s++)
                {
                    int origIdx = indices[s];
                    long entryId = batch[origIdx];

                    if (EmittedBitmap.Contains(entryId))
                        continue;

                    if (locs[s] == -1 || spans[s].Address == null)
                    {
                        EntriesRejected++;
                        continue;
                    }

                    readersArr[packed] = new EntryTermsReader(Llt,
                        Searcher.NullTermsMarkers, Searcher.NonExistingTermsMarkers,
                        spans[s].Address, spans[s].Length, Searcher.DictionaryId, Searcher.VectorFieldsMarkers, scanKey);
                    packedIds[packed] = entryId;
                    packedOrigIdx[packed] = origIdx;
                    packed++;
                }

                int matched = precompiledDelegate(_exec,
                    readersArr.AsSpan(0, packed),
                    packedIds[..packed],
                    packedOrigIdx[..packed]);

                EntriesRejected += packed - matched;

                for (int k = 0; k < matched; k++)
                {
                    passed[packedOrigIdx[k]] = true;
                }

                EntryScanTicks += Stopwatch.GetTimestamp() - t1;

                // Emit in original sort-field order, scan the original batch (which is in sort-field order)
                // and emit only the positions that passed.
                for (int i = 0; i < read && count < remaining; i++)
                {
                    if (passed[i] is false) continue;
                    
                    long id = batch[i];
                    EmittedBitmap.Add(id);
                    EntriesPassedFilter++;
                    matches[count++] = id;
                }
            }

            if (Take > 0 && TotalMatched + count >= Take)
                StoppedReason ??= "ReachedQueryLimits";

            TotalMatched += count;
            return count;
        }
        finally
        {
            Llt.ReleaseCompactKey(ref scanKey);
            ArrayPool<EntryTermsReader>.Shared.Return(readersArr, clearArray: true);
        }
    }
}
