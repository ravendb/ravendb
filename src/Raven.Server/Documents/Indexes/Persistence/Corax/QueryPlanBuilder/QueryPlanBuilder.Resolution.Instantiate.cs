using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Exceptions;
using Sparrow.Json;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static (IQueryMatch Exec, IQueryMatch Inner) Instantiate(
        QueryExecution exec,
        OrderMetadata[] orderByFields,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var compiledPlan = exec.Plan;
        var ctx = new InstantiateContext(compiledPlan, exec, orderByFields, planParams, builderParameters, wantTimings);
        if (compiledPlan.Strategy == ExecutionStrategy.NotEvaluated)
            SelectExecutionStrategy(ref ctx);

        ExecutionStrategy? forced = TryGetForcedStrategy(ctx.PlanParams.QueryParameters); // $rvn_corax_strategy - user can force exec strategy
        ExecutionStrategy effective = forced ?? compiledPlan.Strategy;

        CoraxSortingStrategy? forcedSort = TryGetForcedSortStrategy(ctx.PlanParams.QueryParameters); //  $rvn_corax_sort - use can force sorting strategy

        switch (effective)
        {
            case ExecutionStrategy.CompoundKeyLookup:
            {
                var innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundKeyLookup;
                return (innerMatch, innerMatch);
            }
            case ExecutionStrategy.CompoundSortedScan when orderByFields != null: // no order by -> bitmap is more efficient 
            {
                bool cfEffective = CompoundFieldCostEffective(ref ctx, out long cfEntriesToScan, out long cfBitmapCost, out var cfReason);
                exec.StrategyGateReason = forced is not null ? "forced via $rvn_corax_strategy" : cfReason; 
                if (forced is null && cfEffective == false)
                    goto default; // if this isn't expected to benefit us, just use a bitmap query option

                
                // Single-field order (field2): the DirectScan already emits in that order, elide the wrapper and push a page-bounded Take into the scan.
                bool canElideCompoundSort = orderByFields.Length == 1;
                var innerMatch = ConstructCompoundField(ref ctx, walkerCtx, ctx.Exec.CompoundFieldField2Range, cfEntriesToScan, cfBitmapCost, canElideCompoundSort);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundSortedScan;
                var outer = canElideCompoundSort
                    ? innerMatch // already in field2 order; DirectScan handles Take itself
                    : OrderBy(builderParameters, innerMatch, orderByFields); // this uses SortingMultiMatch, $rvn_corax_sort is inapplicable
                return (outer, innerMatch);
            }
            case ExecutionStrategy.FieldSortedScan when orderByFields != null: // no order by -> bitmap is more efficient
            {
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                string directScanReason = forced is not null ? "forced via $rvn_corax_strategy" : null;
                bool directScanEffective = forced is not null || DirectScanCostEffective(ref ctx, isFullScan, out directScanReason);
                exec.StrategyGateReason = directScanReason;
                if (directScanEffective)
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    if (ConstructDirectScan(ref ctx, walkerCtx, exec.SortDrivingClause, isFullScan, hasTieBreak, directScanReason) is {} innerMatch)
                    {
                        exec.ActualStrategy = ExecutionStrategy.FieldSortedScan;
                        return (innerMatch, innerMatch);
                    }
                }

                goto default;
            }
            case ExecutionStrategy.BitmapPipeline:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
            {
                exec.ActualStrategy = ExecutionStrategy.BitmapPipeline;
                var innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (innerMatch is CompiledQueryMatch forcedScanMatch)
                    forcedScanMatch.ForcedEntryScanGate = TryGetForcedEntryScanGate(ctx.PlanParams.QueryParameters); // $rvn_corax_entry_scan
                // no ordering or already streams its results in right order — return the match as is.
                if (ctx.OrderByFields == null || ctx.Exec.VectorPostFilterProvidesScoreOrder) 
                    return (innerMatch, innerMatch);
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(ctx.Plan, ctx.Exec, seekMatch);
                return (ApplyForcedSort(OrderBy(ctx.BuilderParams, innerMatch, ctx.OrderByFields), forcedSort), innerMatch);
            }
        }

        static void SelectExecutionStrategy(ref InstantiateContext ctx)
        {
            ctx.Plan.DecisionTrail = new();
            ctx.Plan.Strategy = ExecutionStrategy.BitmapPipeline; // if nothing else overrides it

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.CompoundExactCandidate))
            {
                if (TryCreateCompoundExactMatch(ref ctx, out ctx.RejectReason))
                {
                    // No trail entry on success: CompoundKeyLookup has no per-execution cost gate, so there is
                    // no decision to record — the chosen strategy is already surfaced via StrategyCandidate.
                    // A rejection IS recorded below: it explains why a structurally-available optimization
                    // did not apply (encoding failed / boosted clause).
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundKeyLookup;
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundKeyLookup", false, ctx.RejectReason ?? "rejected");
            }

            // No ORDER BY: nothing to decide about a sort strategy, so no trail entry — just stop here.
            if (ctx.OrderByFields is null)
                return;

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.DirectScanCandidate))
            {
                if (TryCreateCompoundFieldMatch(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundSortedScan;
                    ctx.Plan.DecisionTrail.Record("CompoundSortedScan", true, "compound tree scan candidate (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundSortedScan", false, ctx.RejectReason ?? "rejected");

                if (TryCreateSimpleFieldDirectScan(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.FieldSortedScan;
                    ctx.Plan.DecisionTrail.Record("FieldSortedScan", true, "direct tree scan candidate on sort field (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("FieldSortedScan", false, ctx.RejectReason ?? "rejected");
            }

            ctx.Plan.DecisionTrail.Record("BitmapPipeline", true, "bitmap pipeline with SortingMatch fallback");
        }
        
        static bool CompoundFieldCostEffective(ref InstantiateContext ctx, out long entriesToScan, out long bitmapCost, out string reason)
        {
            entriesToScan = 0;
            bitmapCost = 0;
            reason = null;
            var execs  = ctx.Exec.Executions;
            var drivingExec = ctx.Exec.CompoundFieldDrivingClause;

            if (drivingExec.PackedParamValue.IsNone)
            {
                reason = "no driving value for the compound field";
                return false;
            }

            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var field2Range = ctx.Exec.CompoundFieldField2Range;
            int residualCount = 0;
            foreach (var exec in execs)
            {
                bitmapCost += exec.GetEffectiveCardinality(indexSearcher);
                if (exec == drivingExec || exec == field2Range)
                    continue;
                residualCount++;
            }

            long drivingCardinality = drivingExec.GetEffectiveCardinality(indexSearcher);

            if (residualCount == 0)
            {
                // No residual filter: compound(f1,f2)is walked in ORDER BY order and emitted directly - best option
                entriesToScan = Math.Min(drivingCardinality, ctx.BuilderParams.Query.PageSize); // for diagnostics only
                reason = "no residual filter — sorted walk is unconditionally cheaper than build-bitmap-then-sort";
                return true;
            }

            // Residual present: read each entry's fields for the residual, may over-scan
            entriesToScan = ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCardinality, ResolveEffectiveScanPageSize(ctx.BuilderParams), indexSearcher, out long resultsWanted, out double passRate);
            bitmapCost += SurvivorSortCost(EstimateSurvivors(execs, indexSearcher)); // add the bitmap's survivor-sort cost
            string unboundedReason = ctx.WantTimings ? DescribeUnboundedScanTake(ctx.BuilderParams) : null;
            return IsDirectScanCostEffective(entriesToScan, bitmapCost, resultsWanted, passRate, ctx.WantTimings, unboundedReason, out reason);
        }
        
        static bool DirectScanCostEffective(ref InstantiateContext ctx, bool isFullScan, out string directScanReason)
        {
            if (isFullScan)
            {
                directScanReason = "no filter — walking the whole index in sort order";
                return true;
            }

            directScanReason = null;

            var execs = ctx.Exec.Executions;
            var drivingExec = ctx.Exec.SortDrivingClause;
            if (drivingExec is null || drivingExec.PackedParamValue.IsNone)
                return false;

            var indexSearcher = ctx.PlanParams.IndexSearcher;

            if (execs.Count <= 1)
            {
                directScanReason = "sorted index walk with no extra filters to apply, sorting is free";
                return true;
            }

            long bitmapCost = 0;
            foreach (var it in execs)
            {
                bitmapCost += it.GetEffectiveCardinality(indexSearcher);
            }

            // The bitmap path decode the posting lists and SORTS the surviving, estimate that cost too
            bitmapCost += SurvivorSortCost(EstimateSurvivors(execs, indexSearcher));

            // Residual present: scan reads scanned entry's fields and over-scans. Estimate the over-scan amount for the costs
            long drivingCard = drivingExec.GetEffectiveCardinality(indexSearcher);
            var entriesToScan = ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCard, ResolveEffectiveScanPageSize(ctx.BuilderParams), indexSearcher, out long resultsWanted, out double passRate);

            string unboundedReason = ctx.WantTimings ? DescribeUnboundedScanTake(ctx.BuilderParams) : null;
            return IsDirectScanCostEffective(entriesToScan, bitmapCost, resultsWanted, passRate, ctx.WantTimings, unboundedReason, out directScanReason);
        }
        
        static long CalculateDirectCost(long entriesToScan)
        {
            return entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier
                ? long.MaxValue // avoid overflow
                : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        }

        static bool IsDirectScanCostEffective(long entriesToScan, long bitmapCost, long resultsWanted, double passRate, bool wantTimings, string unboundedReason, out string reason)
        {
            reason = null;
            if (entriesToScan > QueryPrimitives.EntryScanCountThreshold)
            {
                if (wantTimings)
                {
                    reason = $"entries_to_scan({entriesToScan:N0}) > cap({QueryPrimitives.EntryScanCountThreshold:N0}) → bitmap{Derivation()}";
                }
                return false;
            }

            long directCost = CalculateDirectCost(entriesToScan);
            bool effective = directCost < bitmapCost;

            if (wantTimings)
            {
                reason = effective ? FormatScanReason() : FormatBitmapReason();
            }

            return effective;

            string Derivation()
            {
                string unbounded = unboundedReason is null ? null : $", page unbounded: {unboundedReason}";
                return $" [results_wanted={resultsWanted:N0}, pass_rate={passRate:P2}{unbounded}]";
            }

            string FormatScanReason() =>
                $"entries_to_scan({entriesToScan:N0}) × {QueryPrimitives.EntryScanCostMultiplier:N0} = {directCost:N0} < bitmap_cost({bitmapCost:N0}) → scan{Derivation()}";

            string FormatBitmapReason() =>
                $"entries_to_scan({entriesToScan:N0}) × {QueryPrimitives.EntryScanCostMultiplier:N0} = {directCost:N0} >= bitmap_cost({bitmapCost:N0}) → bitmap{Derivation()}";
        }
        
        // Independence (product) estimate of the AND intersection: N * Π(card_i / N), clamped to [0, N]. 
        // Published = true (500K) AND Tag = $tag (10K) ~ 5K survivors. 
        static long EstimateSurvivors(List<ClauseExecution> execs, IndexSearcher indexSearcher)
        {
            double n = indexSearcher.NumberOfEntries;
            if (n <= 0)
                return 0;

            double survivors = n;
            foreach (var it in execs)
                survivors *= it.GetEffectiveCardinality(indexSearcher) / n;

            return (long)Math.Clamp(survivors, 0, n);
        }

        static long SurvivorSortCost(long survivors)
            => survivors > long.MaxValue / QueryPrimitives.EntryScanSurvivorSortFactor
                ? long.MaxValue // avoid overflow
                : survivors * QueryPrimitives.EntryScanSurvivorSortFactor;

        static long  ComputeNumberOfEntriesQueryLikelyToScan(List<ClauseExecution> execs,
            ClauseExecution drivingClause, long drivingCard, long pageSize, IndexSearcher indexSearcher,
            out long resultsWanted, out double passRate)
        {
            resultsWanted = Math.Min(drivingCard, pageSize);
            passRate = 1.0; // no selective residual narrows the walk

            long minResidual = long.MaxValue;
            foreach (var exec in execs)
            {
                if (exec == drivingClause) continue;
                minResidual = Math.Min(exec.GetEffectiveCardinality(indexSearcher), minResidual);
            }

            if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
            {
                // here we check what is the pass rate of the most selective residual clause (i.e, 1% of entries matched, etc)
                passRate = (double)minResidual / indexSearcher.NumberOfEntries; // pass rate is (0 .. 1) - ensured by if above
                if (passRate > 0)
                {
                    // if the pass rate is 1%, we have to scan through 10_000 entries to get 100, inflate the results by the pass rate to esitmate.
                    // Getting 10 results with 1% pass rate means - scan 1,000 entries
                    return (long)(resultsWanted / passRate);
                }
            }
            return resultsWanted;
        }
    }

    private const string ForceStrategyParameterName = "rvn_corax_strategy";

    private static ExecutionStrategy? TryGetForcedStrategy(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return null;
        if (queryParameters.TryGet(ForceStrategyParameterName, out string value) == false || string.IsNullOrEmpty(value))
            return null;

        if(Enum.TryParse(value, out ExecutionStrategy result) is false)
            throw new InvalidQueryException(
                $"The reserved query parameter '${ForceStrategyParameterName}' has an unrecognized value '{value}'. Expected one of: {string.Join(", ", Enum.GetNames<ExecutionStrategy>())}");
        return result;

    }

    private const string ForceSortParameterName = "rvn_corax_sort";

    //  $rvn_corax_sort is honored only where a runtime choice exists; a pin that can't apply to the query shape is ignored.
    private static CoraxSortingStrategy? TryGetForcedSortStrategy(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return null;
        if (queryParameters.TryGet(ForceSortParameterName, out string value) == false || string.IsNullOrEmpty(value))
            return null;

        if (Enum.TryParse(value, out CoraxSortingStrategy result) is false)
            throw new InvalidQueryException(
                $"The reserved query parameter '${ForceSortParameterName}' has an unrecognized value '{value}'. Expected one of: {string.Join(", ", Enum.GetNames<CoraxSortingStrategy>())}");
        return result;
    }

    private static IQueryMatch ApplyForcedSort(IQueryMatch match, CoraxSortingStrategy? forcedSort)
    {
        if (forcedSort is { } strategy && match is SortingMatch sortingMatch)
            sortingMatch.ForcedStrategy = strategy;
        return match;
    }

    private const string ForceEntryScanParameterName = "rvn_corax_entry_scan";

    // $rvn_corax_entry_scan: op-index of the gate to force (matches the EntryScanAt the plan reports), -1 to disable every gate
    private static int TryGetForcedEntryScanGate(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return QueryPrimitives.EntryScanGateUnset;
        if (queryParameters.TryGet(ForceEntryScanParameterName, out int value) == false)
            return QueryPrimitives.EntryScanGateUnset;
        return value;
    }
}
