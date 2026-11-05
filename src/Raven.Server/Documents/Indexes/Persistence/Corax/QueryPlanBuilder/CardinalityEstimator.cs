using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class CardinalityEstimator
{
    public static long Estimate(ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
    {
        return EstimateClause(exec);

        long EstimateClause(ClauseExecution e)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            ClauseInfo clause = e.Clause;

            switch (e.ClauseType)
            {
                case ClauseType.MatchAll:
                case ClauseType.MatchNothing:
                    return e.Cardinality; // sentinels carry a preset cardinality (NumberOfEntries / 0); never re-estimated

                case ClauseType.Exists:
                case ClauseType.EndsWith:
                case ClauseType.Search:
                case ClauseType.Regex:
                case ClauseType.Spatial:
                case ClauseType.Vector:
                    return indexSearcher.NumberOfEntries; // Total index size is the only honest data-independent upper bound

                case ClauseType.OrGroup:
                    long orSum = 0;
                    foreach (ClauseExecution subExec in e.SubExecutions)
                    {
                        if (subExec.Cardinality < 0)
                            subExec.Cardinality = EstimateClause(subExec);
                        orSum += subExec.Cardinality;
                    }
                    return Math.Min(orSum, indexSearcher.NumberOfEntries);

                case ClauseType.AndGroup:
                    long andMin = indexSearcher.NumberOfEntries;
                    foreach (ClauseExecution subExec in e.SubExecutions)
                    {
                        if (subExec.Cardinality < 0)
                            subExec.Cardinality = EstimateClause(subExec);
                        andMin = Math.Min(andMin, subExec.Cardinality);
                    }
                    return andMin;
            }

            if (e.PackedParamValue.IsNone) //  A missing (unresolvable) value can't be estimated, so fall back to the whole-index upper bound.
                return indexSearcher.NumberOfEntries;

            switch (e.ClauseType)
            {
                case ClauseType.Equals:
                    return EstimateNumberOfDocumentsUnderSpecificTerm(clause, e);

                case ClauseType.GreaterThan:
                case ClauseType.GreaterThanOrEqual:
                case ClauseType.LessThan:
                case ClauseType.LessThanOrEqual:
                    return EstimateRangeClause(e, e.ClauseType);

                case ClauseType.Between:
                    ClauseType clauseType = ClauseType.Between;
                    if (e.SentinelRewriteType is { } rewrite)
                    {   // A null sentinel bound ("*" / "NULL") rewrites BETWEEN into a half-open range or Exists, the estimate must follow that
                        if (rewrite is ClauseType.Exists)
                            return indexSearcher.NumberOfEntries;
                        clauseType = rewrite;
                    }
                    return EstimateRangeClause(e, clauseType);

                case ClauseType.NotEquals:
                {
                    // NotEquals(X) is MatchAll AndNot Equals(X)
                    long eq = EstimateNumberOfDocumentsUnderSpecificTerm(clause, e);
                    return Math.Max(0, indexSearcher.NumberOfEntries - eq);
                }

                case ClauseType.StartsWith:
                {
                    // StartsWith(prefix) is the bounded prefix range [prefix, successor(prefix))
                    PackedParam p = e.PackedParamValue;
                    if (p.ValueType != PackedParam.TypeString)
                        return indexSearcher.NumberOfEntries; // we cannot encode properly

                    FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx);
                    long startsWith = indexSearcher.EstimateStartsWith(fieldMeta, writer.GetString(p.Param1), out var startsWithBreakdown, clause.RangeEstimateCalibration.Factor);
                    e.RangeEstimate = startsWithBreakdown;
                    return startsWith;
                }

                case ClauseType.In:
                case ClauseType.AllIn:
                    long sum = 0;
                    PackedParam ip = e.PackedParamValue;
                    FieldMetadata meta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx);
                    int start = ip.Param1;
                    int count = e.InTermCount;
                    for (int t = 0; t < count; t++)
                    {
                        sum += ip.ValueType switch
                        {
                            PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetLong(start + t)),
                            PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetDouble(start + t)),
                            _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetString(start + t))
                        };
                    }
                    return Math.Min(sum, indexSearcher.NumberOfEntries);

                default:
                    return indexSearcher.NumberOfEntries;
            }
        }

        long EstimateRangeClause(ClauseExecution e, ClauseType type)
        {
            PackedParam p = e.PackedParamValue; // guaranteed non-None: callers reach here only past the shared guard in EstimateClause
            FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(e.Clause, walkerCtx);

            bool isBetween = type == ClauseType.Between;
            return p.ValueType switch
            {
                PackedParam.TypeLong => EstimateRange(writer.GetLong(p.Param1), isBetween ? writer.GetLong(p.Param2) : 0, long.MinValue, long.MaxValue),
                PackedParam.TypeDouble => EstimateRange(writer.GetDouble(p.Param1), isBetween ? writer.GetDouble(p.Param2) : 0, double.MinValue, double.MaxValue),
                _ => EstimateRange(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, writer.GetString(p.Param1)),
                    isBetween ? indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, writer.GetString(p.Param2)) : default,
                    Slices.BeforeAllKeys, Slices.AfterAllKeys)
            };

            long EstimateRange<T>(T value1, T value2, T min, T max)
            {
                var (low, high, left, right) = type switch
                {
                    ClauseType.Between            => (value1, value2, ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThanOrEqual),
                    ClauseType.GreaterThan        => (value1, max,    ComparisonOperator.GreaterThan,        ComparisonOperator.LessThanOrEqual),
                    ClauseType.GreaterThanOrEqual => (value1, max,    ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThanOrEqual),
                    ClauseType.LessThan           => (min,    value1, ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThan),
                    ClauseType.LessThanOrEqual    => (min,    value1, ComparisonOperator.GreaterThanOrEqual, ComparisonOperator.LessThanOrEqual),
                    _ => throw new ArgumentOutOfRangeException(nameof(type), type, "invalid clause type for range estimation")
                };
                long rangeEstimate = indexSearcher.EstimateMatchesInRange(fieldMeta, low, high, out var rangeBreakdown, left, right, e.Clause.RangeEstimateCalibration.Factor);
                e.RangeEstimate = rangeBreakdown;
                return rangeEstimate;
            }
        }

        long EstimateNumberOfDocumentsUnderSpecificTerm(ClauseInfo clause, ClauseExecution e)
        {
            FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx); // find the relevant analyzer here
            PackedParam p = e.PackedParamValue;
            return p.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetLong(p.Param1)),
                PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetDouble(p.Param1)),
                _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetString(p.Param1))
            };
        }
    }
}
