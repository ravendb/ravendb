using System;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Constants = Corax.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructDirectScan(ref InstantiateContext ctx, ResolutionContext walkerCtx,
        ClauseExecution drivingClause, bool isFullScan, bool hasTieBreak, string reasonForInspection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        string sortFieldName = ctx.WantTimings ? ctx.OrderByFields[0].Field.FieldName.ToString() : null;
        bool forward = ctx.OrderByFields[0].Ascending;

        if (isFullScan == false && ctx.Exec.Plan.DirectScanResidualSet is null)
            return null; // has a WHERE, but no sort-driving clause → bail to the bitmap pipeline.

        var drivingMatchProvider = isFullScan ?
            ResolveFullScanDrivingProvider(ref ctx, forward) :
            ResolveDrivingProvider(ref ctx, walkerCtx, drivingClause, forward);

        if (drivingMatchProvider is not TermsProviderMatch tpm)
            return null; // can happen if we have no entries for this field

        var drivingClauseDescription = ctx.WantTimings ? DrivingClauseDescription(ref ctx) : null;
        
        bool nullFirst = ResolveNullFirst(ctx.OrderByFields[0], ctx.BuilderParams.Index.Configuration.NullsSortMode, forward);

        bool hasResidual = ctx.Exec.Plan.DirectScanResidualSet is { HasPredicates: true };

        long probeTicks = -1; 
        int probeTerms = 0;
        long knownTotal = hasResidual ? -1 : TryResolveDirectScanKnownTotal(ref ctx, walkerCtx, drivingClause, isFullScan, forward, out probeTicks, out probeTerms);
        int take = knownTotal >= 0 ? ctx.BuilderParams.Take : ResolveSortedScanTake(ctx.BuilderParams);

        IQueryMatch drivingMatch = hasTieBreak
            ? BuildSortedDrivingWithTieBreakMatch(ctx, tpm.Provider, tpm.Llt, ctx.BuilderParams.Index.Configuration.NullsSortMode, indexSearcher, nullFirst, take)
            : new SortedDrivingMatch(tpm.Provider, tpm.Llt, ctx.PlanParams.Allocator, indexSearcher, ctx.OrderByFields[0].Field, nullFirst);

        DirectScanMatchBase ds;
        if (hasResidual)
        {
            // Filter every clause EXCEPT the sort-driving clause (walked by the tree).
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.DirectScanResidualSet);
            ds = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: take, precompiledDelegate: ctx.Plan.DirectScanResidualSet.Compiled, token: ctx.Token);
        }
        else
        {   // Nothing to filter, just match.
            ds = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: take, token: ctx.Token)
            {
                KnownExactTotal = knownTotal, KnownTotalProbeTicks = probeTicks, KnownTotalProbeTerms = probeTerms
            };
        }

        if (ctx.WantTimings)
        {
            PopulateDirectScanInspection(ds, sortFieldName, drivingClauseDescription, forward, ctx.Exec.Plan.DirectScanResidualSet?.Predicates,
                isFullScan ? "full index-only scan (no WHERE clause)" : reasonForInspection);
        }
        return ds;
        
        static IQueryMatch ResolveDrivingProvider(ref InstantiateContext ctx, ResolutionContext walkerCtx, ClauseExecution drivingExec, bool forward)
        {
            var match = drivingExec.ClauseType == ClauseType.Equals
                // WHERE PublishedAt = X ORDER BY PublishedAt          - degenerate single-value sort
                // WHERE Category = 'X' ORDER BY Category, Published   - pinned key + real secondary sort
                ? ResolveEqualsClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx)
                // WHERE PublishedAt > X ORDER BY PublishedAt          - half-open range, walked in sort order
                // WHERE Price BETWEEN 10 AND 50 ORDER BY Price DESC   - bounded range, walked backward
                : ResolveRangeClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx);
        
            return match;
        }
        
        static IQueryMatch ResolveEqualsClauseWithDirection(ClauseExecution drivingExec, QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
        {
            var indexSearcher = walkerCtx.IndexSearcher;
            FieldMetadata fieldMeta = ResolveFieldMetadata(drivingExec.Clause, walkerCtx);
            var packed = drivingExec.PackedParamValue;
            return packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, queryExec.LongValues[packed.Param1], queryExec.LongValues[packed.Param1], forward: forward),
                PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, queryExec.DoubleValues[packed.Param1], queryExec.DoubleValues[packed.Param1], forward: forward),
                _ => indexSearcher.BetweenQuery(fieldMeta, queryExec.StringValues[packed.Param1], queryExec.StringValues[packed.Param1], forward: forward)
            };
        }

        static IQueryMatch ResolveRangeClauseWithDirection(ClauseExecution drivingExec, QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
        {
            var indexSearcher = walkerCtx.IndexSearcher;
            FieldMetadata fieldMeta = ResolveFieldMetadata(drivingExec.Clause, walkerCtx);
            var packed = drivingExec.PackedParamValue;

            return drivingExec.ClauseType switch
            {
                ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual or ClauseType.LessThan or ClauseType.LessThanOrEqual
                    => packed.RangeQuery(drivingExec.ClauseType, fieldMeta, indexSearcher, queryExec, forward),
                ClauseType.Between when drivingExec.SentinelRewriteType != null =>
                    ResolveSentinelRewrittenBetween(drivingExec, fieldMeta, indexSearcher, queryExec, forward),
                ClauseType.Between => packed.BetweenQuery(fieldMeta, indexSearcher, queryExec, forward),
                _ => ResolveClause(drivingExec, queryExec, walkerCtx) // fallback
            };
        }

        static IQueryMatch ResolveFullScanDrivingProvider(ref InstantiateContext ctx, bool forward)
        {
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var fieldMeta = ctx.OrderByFields[0].Field;
            var sortFieldType = ctx.OrderByFields[0].FieldType;
            var match = sortFieldType switch
            {
                MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward),
                MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward),
                _ => indexSearcher.ExistsQueryForSortedScan(fieldMeta, forward: forward)
            };
            return match;
        }
        
        static long TryResolveDirectScanKnownTotal(ref InstantiateContext ctx, ResolutionContext walkerCtx, ClauseExecution drivingClause, bool isFullScan, bool forward,
            out long probeTicks, out int probeTerms)
        {
            probeTicks = -1;
            probeTerms = 0;

            if (CanResolveKnownTotal(ctx.BuilderParams) == false)
                return -1; // we have to scan all records anyway, skip this

            if (isFullScan)// A full index-only scan (no WHERE) emits every document 
                return ctx.PlanParams.IndexSearcher.NumberOfEntries; 

            if (ctx.PlanParams.IndexSearcher.HasMultipleTermsInField(ctx.OrderByFields[0].Field))
                return -1; // Multi-valued fields place a document under several terms

            var countMatch = ResolveDrivingProvider(ref ctx, walkerCtx, drivingClause, forward);
            return TryCountPostingsInRange(countMatch, out probeTicks, out probeTerms); // consumes the countMatch
        }

        static void PopulateDirectScanInspection(DirectScanMatchBase ds, string sortFieldName, string drivingClauseDescription, bool forward,
            ScanPredicateInfo[] residualArray, string reason)
        {
            ds.DrivingTreeName = sortFieldName;
            ds.DrivingClause = drivingClauseDescription;
            ds.Direction = forward ? "Forward" : "Backward";
            ds.ResidualDescription = residualArray == null ? null : string.Join(", ", Array.ConvertAll(residualArray, p => $"{p.FieldName} {p.CompareOp}"));
            ds.Reason = reason;
        }

        string DrivingClauseDescription(ref InstantiateContext ctx) =>
            isFullScan 
                ? $"{ctx.OrderByFields[0].Field.FieldName} [all]"
                : $"{drivingClause.Clause.FieldName} {drivingClause.ClauseType}";
    }
}
