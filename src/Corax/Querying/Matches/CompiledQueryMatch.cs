using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>Sort seek hint — the sort field and the value to seek to in it.</summary>
public sealed record SortHint(string FieldName, object Value);

public class CompiledQueryMatch(
    CompiledPlan compiledPlan,
    QueryExecution exec,
    int bitmapCount,
    int opCount,
    IQueryMatch[] resolvedMatches,
    LeafResolveInfo[] leaves,
    IndexSearcher searcher,
    ByteStringContext allocator,
    bool wantTimings,
    CancellationToken token)
    : IBitmapQueryMatch, IDisposable
{
    public readonly ResidualScanIlEmitter.ResidualScanPredicate CompiledEntryPredicate = compiledPlan.EntryScanSet.Compiled;

    public readonly CompiledPlan CompiledPlan = compiledPlan;

    public readonly QueryExecution Exec = exec;

    public SortHint SortHint;

    // all those arrays are parallel to one another - one for each leaf clause in the query
    public readonly IQueryMatch[] ResolvedMatches = resolvedMatches;
    public readonly LeafResolveInfo[] Leaves = leaves;
    public int[] InRangeCounts;
    public long[] Cardinalities;
    public long[] Timings;  
    public long[] ResultCounts;

    public long EntryScanEntriesScanned;
    public long EntryScanEntriesPassed;
    public long EntryScanTiming;

    public readonly IndexSearcher Searcher = searcher;
    public readonly CancellationToken Token = token;

    private RoaringBitmap _bitmapData = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count = -1;

    public RoaringBitmap[] Bitmaps;

    public LowLevelTransaction Llt;

    public int Limit = int.MaxValue;

    /// <summary>
    /// Per-op truncation budget for fill/OR/AND primitives, "unlimited" by default, allows to abort queries midway when we have enough results.
    /// </summary>
    
    // ReSharper disable once FieldCanBeMadeReadOnly.Global - This is being set by generated IL, see: DualEmit.EmitArmOpLimit
    public long OpLimit = long.MaxValue;

    public int ForcedEntryScanGate = Primitives.QueryPrimitives.EntryScanGateUnset;

    public int EntryScanTakenAtOp;

    /// <summary>
    /// When set, the plan's OR/AND fold clones each <see cref="IBitmapQueryMatch"/> leaf's bitmap instead of
    /// consuming (stealing containers from) it, so the leaf's <see cref="BitmapState"/> survives intact for the
    /// score pass. 
    /// </summary>
    public bool PreserveLeavesForScoring;

    public static void MarkPreserveLeavesForScoring(IQueryMatch inner)
    {
        // Unwrap wrappers whose ScoreSorted delegates to the inner match (e.g. spatial PostFilterMatch),
        // so the flag lands on the CompiledQueryMatch whose leaves the score pass re-reads.
        while (inner is PostFilterMatch pf)
            inner = pf.InnerMatch;

        if (inner is CompiledQueryMatch cqm)
            cqm.PreserveLeavesForScoring = true;
    }

    public long Count
    {
        get
        {
            EnsureExecuted();
            return _count;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureExecuted()
    {
        if (_executed is false) Execute();
    }

    public bool IsBoosting
    {
        get
        {
            foreach (var it in ResolvedMatches ?? [])
            {
                if (it is { IsBoosting: true })
                    return true;
            }
            return false;
        }
    }

    public long MinEntryId
    {
        get
        {
            EnsureExecuted();
            long minKey = _bitmapData.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            EnsureExecuted();
            long maxKey = _bitmapData.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public ref RoaringBitmap BitmapState
    {
        get
        {
            EnsureExecuted();
            return ref _bitmapData;
        }
    }

    public int Fill(Span<long> matches)
    {
        EnsureExecuted();
        return _iterator.Fill(ref _bitmapData, matches);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        foreach (var it in ResolvedMatches ?? [])
        {
            it?.Score(matches, scores, boostFactor);
        }
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        foreach (var it in ResolvedMatches ?? [])
        {
            it?.ScoreSorted(matches, scores, boostFactor);
        }
    }

    public void GetTelemetry(out long[] timings, out long[] resultCounts, out int entryScanTakenAtOp)
    {
        timings = Timings;
        resultCounts = ResultCounts;
        entryScanTakenAtOp = EntryScanTakenAtOp;
    }

    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string>
        {
            ["CSharpSource"] = CompiledPlan?.Source ?? "N/A",
            ["CSharpSourceFormatted"] = CompiledPlan?.FormattedSource ?? "N/A"
        };

        if (EntryScanTakenAtOp >= 0)
        {
            parameters["EntryScanAt"] = EntryScanTakenAtOp.ToString();
            if (EntryScanEntriesScanned > 0)
                parameters["EntryScanScanned"] = EntryScanEntriesScanned.ToString();
            if (EntryScanEntriesPassed > 0)
                parameters["EntryScanPassed"] = EntryScanEntriesPassed.ToString();
        }

        if (Timings is { Length: > 0 })
        {
            double tickFreq = System.Diagnostics.Stopwatch.Frequency / 1000.0; // ticks per ms
            for (int i = 0; i < Timings.Length; i++)
            {
                if (Timings[i] > 0)
                    parameters[$"Op{i}_ms"] = (Timings[i] / tickFreq).ToString("F3");
                if (i < ResultCounts.Length && ResultCounts[i] > 0)
                    parameters[$"Op{i}_count"] = ResultCounts[i].ToString();
            }
        }

        var children = new List<QueryInspectionNode>();
        foreach (var it in ResolvedMatches ?? [])
        {
            if (it is null or BitmapMatch) // a bitmap match is consumed by the pipeline and should not be inspected
                continue;
            var node = it.Inspect();
            if (node.IsPostFilter)
                continue;
            children.Add(node);
        }

        return new QueryInspectionNode("CompiledQuery", parameters: parameters, children: children);
    }


    private void Execute()
    {
        if (_executed) return;

        Bitmaps = ArrayPool<RoaringBitmap>.Shared.Rent(bitmapCount);
        Bitmaps[0] = _bitmapData; // main bitmap (owned by this instance)
        for (int i = 1; i < bitmapCount; i++)
        {
            Bitmaps[i] = new RoaringBitmap(allocator);
        }

        Llt = Searcher.Transaction.LowLevelTransaction;

        Timings = wantTimings ? new long[opCount] : null;
        ResultCounts = wantTimings ? new long[opCount] : null;
        EntryScanTakenAtOp = -1;

        try
        {
            CompiledPlan.CompiledDelegate(this);

            _bitmapData = Bitmaps[0];
            _bitmapData.PrepareForReading();
            _count = _bitmapData.ComputeCount();
            _iterator = _bitmapData.GetIterator();
            _executed = true;
        }
        finally
        {
            // we only dispose from 1 and up, 0 is the output for the query
            for (int i = 1; i < bitmapCount; i++)
            {
                Bitmaps[i].Dispose();
            }
            ArrayPool<RoaringBitmap>.Shared.Return(Bitmaps, clearArray: true);
            Bitmaps = null;
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
        Llt = null; // release transaction reference so it is not kept alive longer than needed
    }

}
