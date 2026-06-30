using System;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // Classifies a NormalizedWrapper as a grouped-aggregate or direct-query shape for
    // PowerBIDirectQuery's rewriters.
    internal static class PowerBIShapeClassifier
    {
        // PowerBI's "top 1,000,000 + 1" convention when the wrapper has no LIMIT.
        public const int DefaultDirectQueryLimit = 1_000_001;

        public static bool TryBuildGroupedAggregateShape(NormalizedWrapper wrapper, out GroupedAggregateShape shape)
        {
            shape = null;
            if (wrapper == null)
                return false;

            if (wrapper.Aggregates is not { Count: > 0 } aggregates)
                return false;

            foreach (var agg in aggregates)
            {
                if (string.IsNullOrWhiteSpace(agg.FunctionName) ||
                    string.IsNullOrWhiteSpace(agg.FieldName) ||
                    string.IsNullOrWhiteSpace(agg.OutputColumn))
                    return false;

                if (IsSupportedGroupedAggregateFunction(agg.FunctionName) == false)
                    return false;
            }

            if (wrapper.GroupByColumns is not { Count: > 0 })
                return false;

            if (wrapper.Offset != null && wrapper.Offset != 0)
                return false;

            if (wrapper.OuterWhereClause != null)
            {
                var matchedAnyAggregateOutput = false;
                foreach (var agg in aggregates)
                {
                    if (TryIsOuterAggregateNotNullFilter(wrapper.OuterWhereClause, expectedName: agg.OutputColumn))
                    {
                        matchedAnyAggregateOutput = true;
                        break;
                    }
                }

                if (matchedAnyAggregateOutput == false)
                    return false;
            }

            shape = new GroupedAggregateShape(
                GroupByFields: wrapper.GroupByColumns,
                Aggregates: aggregates,
                OrderByCols: wrapper.OrderByColumns,
                OrderByDescFlags: wrapper.OrderByDescFlags,
                Limit: wrapper.Limit ?? DefaultDirectQueryLimit);
            return true;
        }

        public static bool TryBuildDirectQueryShape(NormalizedWrapper wrapper, out DirectQueryShape shape)
        {
            shape = null;
            if (wrapper == null)
                return false;

            if (wrapper.OuterProjectedColumns is not { Count: > 0 })
                return false;

            if (wrapper.GroupByColumns is not { Count: > 0 })
                return false;

            // Aggregate visuals (avg/min/max, or the sum/count shapes the grouped-aggregate path
            // already rejected) must not be treated as a tuple-distinct projection: the aggregate
            // output alias would be grouped/selected as a document field, yielding empty/garbage.
            // Reject so they fall through to the diagnoser.
            if (wrapper.Aggregates is { Count: > 0 })
                return false;

            var limit = wrapper.Limit ?? DefaultDirectQueryLimit;
            shape = new DirectQueryShape(wrapper.OuterProjectedColumns, limit);
            return true;
        }

        private static bool TryIsOuterAggregateNotNullFilter(Node whereClause, string expectedName)
        {
            if (whereClause == null || string.IsNullOrWhiteSpace(expectedName))
                return false;

            if (TryExtractNotNullTest(whereClause, out var colRef) == false)
                return false;

            if (PowerBIWrapperRecognizer.TryExtractOuterUnderscoreQualifiedColumn(colRef, out var col) == false)
                return false;

            return string.Equals(col, expectedName, StringComparison.OrdinalIgnoreCase);
        }

        private static bool TryExtractNotNullTest(Node where, out ColumnRef colRef)
        {
            colRef = null;

            // Direct: "_"."a0" is not null
            if (where.NullTest != null)
            {
                var nt = where.NullTest;
                if (nt.Nulltesttype == NullTestType.IsNotNull)
                {
                    colRef = nt.Arg?.ColumnRef;
                    return colRef != null;
                }

                return false;
            }

            // NOT( "_"."a0" is null )
            var be = where.BoolExpr;
            if (be?.Boolop != BoolExprType.NotExpr || be.Args is not { Count: 1 })
                return false;

            var inner = be.Args[0];
            var innerNt = inner?.NullTest;
            if (innerNt == null)
                return false;

            if (innerNt.Nulltesttype != NullTestType.IsNull)
                return false;

            colRef = innerNt.Arg?.ColumnRef;
            return colRef != null;
        }

        // The emitter currently understands only sum + count. min/max/avg would be valid SQL but
        // fall through to the next dispatch tier.
        private static bool IsSupportedGroupedAggregateFunction(string name) =>
            string.Equals(name, "sum", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "count", StringComparison.OrdinalIgnoreCase);
    }
}
