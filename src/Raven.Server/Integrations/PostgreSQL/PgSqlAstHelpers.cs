using System;
using System.Collections.Generic;
using System.Globalization;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL
{
    internal static class PgSqlAstHelpers
    {
        // Walks past TypeCast / RelabelType / AExpr wrappers to find a T payload.
        public static T UnwrapThroughHarmlessNodes<T>(Node node, Func<Node, T> picker) where T : class
        {
            while (node != null)
            {
                var picked = picker(node);
                if (picked != null)
                    return picked;

                if (node.TypeCast != null)
                {
                    node = node.TypeCast.Arg;
                    continue;
                }

                if (node.RelabelType != null)
                {
                    node = node.RelabelType.Arg;
                    continue;
                }

                if (node.AExpr != null)
                {
                    node = node.AExpr.Lexpr ?? node.AExpr.Rexpr;
                    continue;
                }

                break;
            }

            return null;
        }

        // Tolerates all three AConst kinds: Ival, Sval, Fval.
        public static bool TryReadNonNegativeIntConst(Node node, out int value)
        {
            value = 0;

            var c = node?.AConst;
            if (c == null)
                return false;

            if (c.Ival != null)
            {
                value = (int)c.Ival.Ival;
                return value >= 0;
            }

            if (c.Sval != null && int.TryParse(c.Sval.Sval, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                return value >= 0;

            if (c.Fval != null && int.TryParse(c.Fval.Fval, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                return value >= 0;

            return false;
        }

        public static bool IsPowerBiWrapperAlias(string alias)
        {
            if (string.IsNullOrWhiteSpace(alias))
                return false;

            return string.Equals(alias, "$Table", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(alias, "_", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(alias, "rows", StringComparison.OrdinalIgnoreCase);
        }

        public static bool TryGetSingleRangeVarFromClause(SelectStmt selectStmt, out RangeVar rangeVar)
        {
            rangeVar = null;

            if (selectStmt?.FromClause is not { Count: 1 } fromClause)
                return false;

            rangeVar = fromClause[0]?.RangeVar;
            return rangeVar?.Schemaname != null && rangeVar.Relname != null;
        }

        public static bool IsSelectColumn(Node node, string expectedColumn)
        {
            var colRef = node?.ResTarget?.Val?.ColumnRef;
            return colRef?.Fields is { Count: 1 } fields &&
                   fields[0].String?.Sval?.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase) == true;
        }

        public static bool IsOrderByAsc(Node node, string expectedColumn)
        {
            var sortBy = node?.SortBy;
            if (sortBy == null)
                return false;

            if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                return false;

            var colRef = sortBy.Node?.ColumnRef;
            if (colRef?.Fields is not { Count: 1 } fields)
                return false;

            return fields[0].String?.Sval?.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase) == true;
        }

        public static bool ProjectionSubsetOf(IList<Node> targetList, HashSet<string> produceable)
        {
            if (targetList == null || targetList.Count == 0)
                return false;

            foreach (var node in targetList)
            {
                var rt = node?.ResTarget;
                if (rt == null)
                    return false;

                var colRef = rt.Val?.ColumnRef;
                if (colRef != null)
                {
                    var name = colRef.Fields?[^1]?.String?.Sval;
                    if (string.IsNullOrWhiteSpace(name) || produceable.Contains(name) == false)
                        return false;
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(rt.Name) || produceable.Contains(rt.Name) == false)
                        return false;
                }
            }

            return true;
        }
    }
}
