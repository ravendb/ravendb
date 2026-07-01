using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal sealed class PlanEmitter
{
    private readonly List<PlanOp> _ops = [];
    private int _nextRangeIdx; // index for ctx.InRangeCounts[idx] at runtime.
    private int _matchIndex;

    private int _nextScratch = QueryPrimitives.EphemeralBitmapSlot + 1;
    private int _maxScratchUsed = QueryPrimitives.EphemeralBitmapSlot;

    public static (PlanOp[] Ops, int RequiredBitmaps) Emit(PlanTemplate template, List<ClauseExecution> executions, PlanParameters planParams, bool scanEligible)
    {
        if (executions.Count is 0) // a genuinely clause-less query (no WHERE) — match every doc.
            return ([new PlanOp { Kind = PlanOpKind.FillFromMatch, ParamIndex = 0 }], 2);

        var emitter = new PlanEmitter();
        var (ops, bitmaps) = template.IsOr ? emitter.EmitOrPlan(executions) : emitter.EmitAndPlan(executions, scanEligible);
        if (planParams.HasBoost)
        {
            // we require query match for boost, because the other options cannot compute it
            for (int i = 0; i < ops.Length; i++)
            {
                ops[i].Kind = ToMatchVariant(ops[i].Kind);
            }
        }
        return (ops, bitmaps);
    }

    private (PlanOp[] Ops, int RequiredBitmaps) Complete()
    {
        _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDone });
        return (_ops.ToArray(), Math.Max(2, _maxScratchUsed + 1));
    }

    private ScratchSlotScope AllocateScratchSlot(out int slot)
    {
        slot = _nextScratch++;
        if (slot > _maxScratchUsed)
            _maxScratchUsed = slot;
        return new ScratchSlotScope(this);
    }

    private readonly struct ScratchSlotScope(PlanEmitter emitter) : IDisposable
    {
        public void Dispose() => emitter._nextScratch--;
    }

    private (PlanOp[] Ops, int RequiredBitmaps) EmitOrPlan(List<ClauseExecution> executions)
    {
        Debug.Assert(executions.Count > 0);

        // Negation sorts last, so all negated clauses are grouped together, and we can fold them using De Morgan -> complement 
        // i.e: Status != 'Minor' or  Age > 18 or Credit != 'Bad' is sorted to: Age > 18 pr Status != 'Minor' or Credit != 'Bad'  
        bool first = true;
        int i = 0;
        while (i < executions.Count)
        {
            int runEnd = FoldableNegatedRunEnd(executions, i);
            if (runEnd - i >= 2) // ≥2 foldable negations: collapse N FillAllEntries into one complement
            {
                // Full query would effectively be Age > 18 or NOT (Status = 'Minor' AND Credit = 'Bad')
                // The OR clause would be: AllEntries AND NOT (Status = 'Minor' AND Credit = 'Bad')
                EmitFoldedNegatedRun(executions, i, runEnd, first);
                first = false;
                i = runEnd;
                continue;
            }

            EmitClauseInto(executions[i], first ? MergeKind.Fill : MergeKind.OrInto, suppressEarlyExit: true, destSlot: 0);
            first = false;
            i++;
        }

        return Complete();
    }

    // Length of the contiguous run of foldable negated leaves starting at <paramref name="start"/>.
    private static int FoldableNegatedRunEnd(List<ClauseExecution> executions, int start)
    {
        int j = start;
        while (j < executions.Count && IsFoldableNegatedLeaf(executions[j]))
            j++;
        return j;
    }

    private static bool IsFoldableNegatedLeaf(ClauseExecution e)
    {
        if (e.IsSentinel) // a collapse sentinel has no positive form to intersect; never fold it
            return false;
        if (e.Clause.IsOrChainNotEquals == false)
            return false;
        return e.ClauseType is not (ClauseType.OrGroup or ClauseType.AndGroup);
    }

    // ≥2 members and every one of them is a foldable negated leaf (i.e. the run covers the whole list).
    private static bool CanFoldNegatedOr(List<ClauseExecution> executions) =>
        executions.Count >= 2 && FoldableNegatedRunEnd(executions, 0) == executions.Count;
    
    private void EmitFoldedNegatedRun(List<ClauseExecution> executions, int from, int to, bool isFirst)
    {
        if (isFirst)
        {
            // slot 0 is empty: build the complement straight into it.
            EmitComplementOfIntersection( destSlot: 0);
            return;
        }

        // slot 0 holds the running OR accumulator. Build the complement in a fresh scratch slot
        // (so the complement's leading FillAllEntries can't clobber the accumulator), then OR it in.
        using var _ = AllocateScratchSlot(out int compSlot);
        EmitComplementOfIntersection(compSlot);
        _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = compSlot });
        
        void EmitComplementOfIntersection(int destSlot)
        {
            using var s = AllocateScratchSlot(out int xSlot);
            EmitPositiveIntersection(executions, from, to, xSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = xSlot });
        }
    }

    // Build X = exec[from] ∧ … ∧ exec[to-1] into xSlot (Fill then AndInto, early-exit suppressed so a
    // partial/empty intersection can't short-circuit). Shared head of both De Morgan complement paths.
    private void EmitPositiveIntersection(List<ClauseExecution> executions, int from, int to, int xSlot)
    {
        EmitPositiveForm(executions[from], MergeKind.Fill, suppressEarlyExit: true, xSlot);
        for (int i = from + 1; i < to; i++)
            EmitPositiveForm(executions[i], MergeKind.AndInto, suppressEarlyExit: true, xSlot);
    }


    private (PlanOp[] Ops, int RequiredBitmaps) EmitAndPlan(List<ClauseExecution> executions, bool scanEligible)
    {
        foreach (var cur in executions)
        {
            if (cur.ClauseType == ClauseType.MatchNothing)
            {
                // MatchNothing ∧ anything = MatchNothing
                _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 0 });
                return Complete();
            }
        }

        var e0 = executions[0];
        if (e0.IsNegated)
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries });

        EmitClauseInto(e0, e0.IsNegated ? MergeKind.AndNotInto : MergeKind.Fill, suppressEarlyExit: false, destSlot: 0);

        for (int i = 1; i < executions.Count; i++)
        {
            var cur = executions[i];

            if (scanEligible && cur.IsSentinel == false) // if we can, check if we can move to entry scan after the first check
            {
                _ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.MaybeEntryScan,
                    ParamIndex = _matchIndex
                });
            }

            MergeKind merge = cur.IsNegated ? MergeKind.AndNotInto : MergeKind.AndInto;

            EmitClauseInto(cur, merge, suppressEarlyExit: true, destSlot: 0);
            if (cur.IsNegated is false) // when we have 0 results, early exit
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDoneIfEmpty, BitmapLocal = 0 });
            }
        }

        return Complete();
    }

    private void EmitClauseInto(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        if (exec.IsSentinel)
        {
            EmitSentinelInto(exec, merge, destSlot);
            return;
        }

        if (exec.Clause.IsOrChainNotEquals)
        {
            EmitNegatedLeafInto(exec, merge, destSlot);
            return;
        }

        EmitPositiveForm(exec, merge, suppressEarlyExit, destSlot);
    }

   
    private void EmitSentinelInto(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        switch (exec.ClauseType, merge)
        {
            case (ClauseType.MatchAll, MergeKind.Fill):
            case (ClauseType.MatchAll, MergeKind.OrInto):           // x ∨ ALL = ALL
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
                break;

            case (ClauseType.MatchAll, MergeKind.AndNotInto):       // x \ ALL = ∅ — defensive; MatchAll is never negated.
            case (ClauseType.MatchNothing, MergeKind.Fill):         // empty seed
            case (ClauseType.MatchNothing, MergeKind.AndInto):      // x ∧ ∅ = ∅
                _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = destSlot });
                break;
            
            case (ClauseType.MatchAll, MergeKind.AndInto):          // x ∧ ALL = x
            case (ClauseType.MatchNothing, MergeKind.OrInto):       // x ∨ ∅ = x
            case (ClauseType.MatchNothing, MergeKind.AndNotInto):   // x \ ∅ = x
                break;
        }
    }
    
    private void EmitPositiveForm(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        switch (exec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup when exec.Clause.SubClauses is { Count: > 0 }:
                EmitGroupInto(exec, exec.SubExecutions, merge, suppressEarlyExit, destSlot);
                break;
            case ClauseType.In:
                EmitInLeaf(exec, merge, destSlot);
                break;
            case ClauseType.AllIn:
                EmitAllInLeaf(exec, merge, suppressEarlyExit, destSlot);
                break;
            default:
                _ops.Add(new PlanOp
                {
                    Kind = ToPlanOpKind(merge, QueryPlanBuilder.GetDispatch(exec)),
                    ParamIndex = _matchIndex++,
                    BitmapLocal = destSlot,
                    SkipEarlyExit = merge == MergeKind.AndInto && suppressEarlyExit,
                    DebugLabel = Label(exec)
                });
                break;
        }
    }

    private void EmitGroupInto(ClauseExecution exec, List<ClauseExecution> subExecs, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        if (merge == MergeKind.Fill)
        {
            EmitGroupContents(exec, subExecs, suppressEarlyExit, destSlot);
            return;
        }

        // De Morgan in an AND context: an all-negated OR sub-group is ¬(A ∧ B ∧ …). 
        if (exec.ClauseType == ClauseType.OrGroup && merge is MergeKind.AndInto or MergeKind.AndNotInto && CanFoldNegatedOr(subExecs))
        {
            EmitFoldedNegatedOrGroupIntoAccumulator(subExecs, merge, destSlot);
            return;
        }

        // Build the group into a fresh scratch slot, then merge it into destSlot directly — which holds the accumulator.
        using var _ = AllocateScratchSlot(out int groupSlot);
        EmitGroupContents(exec, subExecs, suppressEarlyExit: true, groupSlot);

        switch (merge)
        {
            case MergeKind.OrInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
                break;
            case MergeKind.AndInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
                break;
            case MergeKind.AndNotInto:
                // destSlot \ groupSlot — accumulator stays put, group is subtracted, so no operand swap.
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
                break;
        }
    }

    // Fold an all-negated OR sub-group (¬A ∨ ¬B ∨ … = ¬(A ∧ B ∧ …)) into the accumulator without materializing the universe
    private void EmitFoldedNegatedOrGroupIntoAccumulator(List<ClauseExecution> subExecs, MergeKind merge, int destSlot)
    {
        using var _ = AllocateScratchSlot(out int xSlot);

        EmitPositiveIntersection(subExecs, 0, subExecs.Count, xSlot);

        _ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
            BitmapLocal = destSlot,
            ParamIndex2 = xSlot
        });
    }

    private void EmitGroupContents(ClauseExecution exec, List<ClauseExecution> subExecs, bool suppressEarlyExit, int destSlot)
    {
        bool isOr = exec.ClauseType != ClauseType.OrGroup;
        bool firstNegated = isOr && subExecs[0].IsNegated;
        if (firstNegated)
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
        var followupAction = isOr ? MergeKind.AndInto : MergeKind.OrInto;
        EmitClauseInto(subExecs[0], firstNegated ? MergeKind.AndNotInto : MergeKind.Fill, suppressEarlyExit, destSlot);
        for (int i = 1; i < subExecs.Count; i++)
        {
            MergeKind kind = isOr && subExecs[i].IsNegated ? MergeKind.AndNotInto : followupAction;
            EmitClauseInto(subExecs[i], kind, suppressEarlyExit, destSlot);
        }
    }

    private static PlanOpKind ToPlanOpKind(MergeKind merge, MatchDispatch dispatch) => (merge, dispatch) switch
    {
        (MergeKind.Fill, MatchDispatch.PostingList)       => PlanOpKind.FillFromPostingSource,
        (MergeKind.Fill, MatchDispatch.TreeScan)          => PlanOpKind.FillFromTreeScan,
        (MergeKind.Fill, _)                               => PlanOpKind.FillFromMatch,

        (MergeKind.OrInto, MatchDispatch.PostingList)     => PlanOpKind.OrFromPostingSource,
        (MergeKind.OrInto, MatchDispatch.TreeScan)        => PlanOpKind.OrFromTreeScan,
        (MergeKind.OrInto, _)                             => PlanOpKind.OrFromMatch,

        (MergeKind.AndInto, MatchDispatch.PostingList)    => PlanOpKind.AndFromPostingSource,
        (MergeKind.AndInto, MatchDispatch.TreeScan)       => PlanOpKind.AndFromTreeScan,
        (MergeKind.AndInto, _)                            => PlanOpKind.AndFromMatch,

        (MergeKind.AndNotInto, MatchDispatch.PostingList) => PlanOpKind.AndNotFromPostingSource,
        (MergeKind.AndNotInto, MatchDispatch.TreeScan)    => PlanOpKind.AndNotFromTreeScan,
        (MergeKind.AndNotInto, _)                         => PlanOpKind.AndNotFromMatch,

        _ => throw new InvalidOperationException($"Unhandled MergeKind: {merge} / {dispatch}")
    };

    private static PlanOpKind ToMatchVariant(PlanOpKind kind) => kind switch
    {
        PlanOpKind.FillFromPostingSource   or PlanOpKind.FillFromTreeScan   => PlanOpKind.FillFromMatch,
        PlanOpKind.AndFromPostingSource    or PlanOpKind.AndFromTreeScan    => PlanOpKind.AndFromMatch,
        PlanOpKind.OrFromPostingSource     or PlanOpKind.OrFromTreeScan     => PlanOpKind.OrFromMatch,
        PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan => PlanOpKind.AndNotFromMatch,
        PlanOpKind.InRangeFromPostingSource                                 => PlanOpKind.InRangeFromMatch,
        PlanOpKind.AllInRangeFromPostingSource                              => PlanOpKind.AllInRangeFromMatch,
        _                                                                   => kind
    };

    /// <summary>IN clause leaf — logically (term0 ∪ term1 ∪ … ∪ termN).</summary>
    private void EmitInLeaf(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        if (merge is MergeKind.Fill or MergeKind.OrInto)
        {
            var firstKind = merge == MergeKind.Fill ? PlanOpKind.FillFromPostingSource : PlanOpKind.OrFromPostingSource;
            EmitCommonInOps(exec.InTermCount, destSlot, firstKind, PlanOpKind.InRangeFromPostingSource, suppressEarlyExit: false, Label(exec));
            return;
        }

        var ephemeralBitmap = QueryPrimitives.EphemeralBitmapSlot;
        EmitCommonInOps(exec.InTermCount, ephemeralBitmap, PlanOpKind.FillFromPostingSource, PlanOpKind.InRangeFromPostingSource, suppressEarlyExit: false, Label(exec));
        _ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndBitmaps : PlanOpKind.AndNotBitmaps,
            BitmapLocal = destSlot,
            ParamIndex2 = ephemeralBitmap
        });
    }

    /// <summary>AllIn clause leaf — logically (term0 ∩ term1 ∩ … ∩ termN).</summary>
    private void EmitAllInLeaf(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        if (merge == MergeKind.Fill)
        {
            EmitCommonInOps(exec.InTermCount, destSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AllInRangeFromPostingSource, suppressEarlyExit, Label(exec));
            return;
        }

        using var _ = AllocateScratchSlot(out int saveSlot);

        EmitCommonInOps(exec.InTermCount, saveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AllInRangeFromPostingSource, suppressEarlyExit: true, Label(exec));

        switch (merge)
        {
            case MergeKind.OrInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndNotInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
        }
    }

    private void EmitNegatedLeafInto(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        Debug.Assert(merge is MergeKind.Fill or MergeKind.OrInto,
            $"IsOrChainNotEquals only appears in OR chains; got merge={merge}");

        if (merge == MergeKind.Fill)
        {
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
            EmitComplementBody(exec, destSlot);
            return;
        }

        // OR into an existing accumulator: build the complement (ALL \ positive) in a fresh scratch slot, then OR it into destSlot.
        using var _ = AllocateScratchSlot(out int compSlot);

        _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = compSlot });
        EmitComplementBody(exec, compSlot);
        _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = compSlot });
    }

    // Subtract the clause's positive form: destSlot = destSlot \ positive
    private void EmitComplementBody(ClauseExecution exec, int destSlot)
    {
        switch (exec.ClauseType)
        {
            // IN/AllIn have no fused negated primitive: the set must be assembled into a bitmap and then
            // subtracted. That assemble-then-subtract is exactly the leaf emitters' AndNotInto arm.
            case ClauseType.In:
                EmitInLeaf(exec, MergeKind.AndNotInto, destSlot);
                return;
            case ClauseType.AllIn:
                EmitAllInLeaf(exec, MergeKind.AndNotInto, suppressEarlyExit: true, destSlot);
                return;
            default:
                _ops.Add(new PlanOp
                {
                    Kind = ToPlanOpKind(MergeKind.AndNotInto, QueryPlanBuilder.GetDispatch(exec)),
                    ParamIndex = _matchIndex++,
                    BitmapLocal = destSlot,
                    DebugLabel = Label(exec)
                });
                break;
        }
    }

    private void EmitCommonInOps(int inTermCount, int bitmapLocal, PlanOpKind firstKind, PlanOpKind secondKind, bool suppressEarlyExit, string label)
    {
        int totalSlots = inTermCount + 1;
        _ops.Add(new PlanOp
        {
            Kind = firstKind,
            ParamIndex = _matchIndex,
            BitmapLocal = bitmapLocal,
            DebugLabel = label
        });

        _ops.Add(new PlanOp
        {
            Kind = secondKind,
            ParamIndex = _matchIndex + 1,
            ParamIndex2 = _nextRangeIdx++,
            BitmapLocal = bitmapLocal,
            SkipEarlyExit = suppressEarlyExit, // Defaults to false for EmitInOps
            DebugLabel = label
        });

        _matchIndex += totalSlots;
    }

    private static string Label(ClauseExecution exec)
    {
        string field = exec.Clause?.FieldName;
        var compareOp = exec.ClauseType switch
        {
            ClauseType.Equals             => "==",
            ClauseType.NotEquals          => "!=",
            ClauseType.GreaterThan        => ">",
            ClauseType.GreaterThanOrEqual => ">=",
            ClauseType.LessThan           => "<",
            ClauseType.LessThanOrEqual    => "<=",
            ClauseType.Between            => "BETWEEN",
            ClauseType.Exists             => "EXISTS",
            ClauseType.StartsWith         => "STARTS WITH",
            ClauseType.EndsWith           => "ENDS WITH",
            ClauseType.In                 => "IN",
            ClauseType.AllIn              => "ALL IN",
            { } s                         => s.ToString() 
        };
        return field is null ? exec.ClauseType.ToString() : $"{field} [{compareOp}]";
    }
}
