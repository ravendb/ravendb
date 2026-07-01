using System;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Voron;
using Constants = Corax.Constants;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructCompoundField(ref InstantiateContext ctx, ResolutionContext walkerCtx, ClauseExecution field2Range, long entriesToScan, long bitmapCost, bool canElideCompoundSort)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var drivingClause = ctx.Exec.CompoundFieldDrivingClause;

        var packed = drivingClause.PackedParamValue;

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
            return null;

        string field1Name = drivingClause.Clause.FieldName;
        string compoundFieldName = ctx.Exec.Plan.Template.CompoundFieldName;
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        Slice analyzedPrefix = BuildField1Prefix(ref ctx, field1Name, packed, out string field1ValueStr);
        if (analyzedPrefix.HasValue == false || analyzedPrefix.Size > byte.MaxValue) // if too long, cannot be used for compound
            return null; // fall back to bitmap

        bool forward = ctx.OrderByFields[0].Ascending;

        bool usedCompositeRange = false;
        Slice compositeLow = default, compositeHigh = default;
        if (CreateDrivingMatch(ref ctx) is not { } drivingMatch)
            return null; // unsupported shape (e.g. backward prefix scan) — fall back to the bitmap pipeline

        // When the sort wrapper is elided, the output IS the final order, but TermsProviderMatch returns entry-id order
        // so wrap it in a term-by-term walk that keeps field2 order (when not eliding, the outer SortingMatch re-sorts anyway).
        if (canElideCompoundSort && drivingMatch is TermsProviderMatch tpm)
            drivingMatch = new SortedDrivingMatch(tpm.Provider, tpm.Llt, ctx.PlanParams.Allocator);

        bool hasResidual = ctx.Exec.Plan.CompoundFieldResidualSet is { HasPredicates: true };

        long knownProbeTicks = -1; 
        int knownProbeTerms = 0;
        long knownTotal = hasResidual ? -1 : TryResolveCompoundKnownTotal(ref ctx);

        // When knownTotal resolves the scan is page-bounded even under statistics, compound sort still requires SortingMultiMatch (and thus full results)
        int take = canElideCompoundSort
            ? (knownTotal >= 0 ? ctx.BuilderParams.Take : ResolveSortedScanTake(ctx.BuilderParams))
            : Constants.IndexSearcher.TakeAll;

        DirectScanMatchBase directScan;
        if (hasResidual)  // Filter every clause EXCEPT {driving, field2Range} (both enforced by the compound key).
        {
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.CompoundFieldResidualSet);
            directScan = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: take, precompiledDelegate: ctx.Plan.CompoundFieldResidualSet.Compiled);
        }
        else // nothing to filter, just scan...
        {   
            directScan = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: take)
            {
                KnownExactTotal = knownTotal, KnownTotalProbeTicks = knownProbeTicks, KnownTotalProbeTerms = knownProbeTerms
            };
        }

        if (ctx.WantTimings) // only used when we use include timings()
            SetDirectScanPropertiesForIntrospection(ref ctx);

        return directScan;

        IQueryMatch CreateDrivingMatch(ref InstantiateContext context)
        {
            string fieldName = context.Exec.Plan.Template.CompoundFieldSortName;
            if (field2Range is not null && // can we do a range filter on field2 ?
                TryBuildCompositeRangeKeys(ref context, analyzedPrefix, fieldName, field2Range, out var lowSlice, out var highSlice))
            {
                usedCompositeRange = true;
                compositeLow = lowSlice;
                compositeHigh = highSlice;
                return BuildCompositeRangeMatch(lowSlice, highSlice);
            }

            // No field2 narrowing: run a prefix scan on field1 and let entry-scan residuals filter the rest.
            return indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: forward,
                validatePostfixLen: true);
        }

        IQueryMatch BuildCompositeRangeMatch(Slice low, Slice high)
        {
            return field2Range.Clause.ClauseType switch
            {
                ClauseType.GreaterThan => indexSearcher.RangeBuilder<Range.Exclusive, Range.Inclusive>(compoundFieldMeta, low, high, forward),
                ClauseType.LessThan => indexSearcher.RangeBuilder<Range.Inclusive, Range.Exclusive>(compoundFieldMeta, low, high, forward),
                _ => indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(compoundFieldMeta, low, high, forward)
            };
        }

        long TryResolveCompoundKnownTotal(ref InstantiateContext context)
        {
            if (canElideCompoundSort == false || CanResolveKnownTotal(context.BuilderParams) == false)
                return -1; // these require that we'll get the full result set anyway

            if (usedCompositeRange) // all results are in [low, high], we can just check the range
            {
                if (indexSearcher.HasMultipleTermsInField(compoundFieldMeta))
                    return -1; // but we cannot do that if we have multiple terms per document (need to de-dupe that)

                var countMatch = BuildCompositeRangeMatch(compositeLow, compositeHigh); // separate match, because count will consume it
                return TryCountPostingsInRange(countMatch, out knownProbeTicks, out knownProbeTerms);
            }

            // Bare field1-equality prefix (no field2 filter), we can just count that directly.
            // where Lang = 'en' order by Date desc - we get the already computed cardinality of Lang = 'en'
            if (field2Range is null
                && drivingClause.Clause.ClauseType == ClauseType.Equals
                && drivingClause.Cardinality > 0)
            {
                return drivingClause.Cardinality;
            }

            return -1;
        }

        void SetDirectScanPropertiesForIntrospection(ref InstantiateContext context)
        {
            directScan.DrivingTreeName = compoundFieldName;
            directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
            directScan.SeekBound = usedCompositeRange
                ? $"'{field1ValueStr}' + {field2Range.Clause.FieldName} {field2Range.Clause.ClauseType} (composite range)"
                : $"'{field1ValueStr}' (prefix, validatePostfixLen)";
            directScan.Direction = context.OrderByFields[0].Ascending ? "Forward" : "Backward";
            directScan.ResidualDescription = context.Exec.Plan.CompoundFieldResidualSet?.Predicates is { } cfr ? 
                FormatPredicates(cfr) 
                : null;
            directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        }

        string FormatPredicates(ScanPredicateInfo[] cfr) => string.Join(", ", Array.ConvertAll(cfr, p => $"{p.FieldName} {p.CompareOp.ToOperator()}"));
    }
}
