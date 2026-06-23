using System;
using System.Collections.Generic;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // Recognizes PowerBI's outer-wrapper SQL and normalizes it into a NormalizedWrapper that the
    // shape classifier (PowerBIShapeClassifier) and the RQL rewriters in PowerBIDirectQuery can
    // consume.
    internal static class PowerBIWrapperRecognizer
    {
        public static bool TryNormalize(SelectStmt selectStmt, out NormalizedWrapper wrapper)
        {
            wrapper = null;

            if (selectStmt == null)
                return false;

            // Flat grouped shape: GROUP BY at outermost level, no outer "_" wrapper.
            if (selectStmt.GroupClause is { Count: > 0 })
            {
                int? limitFlat = null;
                if (selectStmt.LimitCount != null)
                {
                    if (TryExtractLimit(selectStmt, out var l) == false)
                        return false;
                    limitFlat = l;
                }

                int? offsetFlat = null;
                if (selectStmt.LimitOffset != null)
                {
                    if (TryExtractOffset(selectStmt, out var o) == false)
                        return false;
                    offsetFlat = o;
                }

                if (TryExtractGroupByColumns(selectStmt, out var groupByColsFlat) == false)
                    return false;

                TryExtractAggregates(selectStmt, out var aggregatesFlat);

                if (TryExtractSortClauseToOrderBy(selectStmt, out var orderByColsFlat, out var orderByDescFlagsFlat) == false)
                    return false;

                wrapper = new NormalizedWrapper(
                    OuterProjectedColumns: new List<string>(groupByColsFlat),
                    GroupByColumns: groupByColsFlat,
                    OrderByColumns: orderByColsFlat,
                    OrderByDescFlags: orderByDescFlagsFlat,
                    OuterWhereClause: selectStmt.WhereClause,
                    Limit: limitFlat,
                    Offset: offsetFlat,
                    Aggregates: aggregatesFlat,
                    IntermediateWheres: new List<IntermediateWhere>());
                return true;
            }

            if (TryExtractProjectedColumnsFromAnyWrapperLevel(selectStmt, out var outerCols) == false)
                return false;

            int? limit = null;
            if (selectStmt.LimitCount != null)
            {
                if (TryExtractLimit(selectStmt, out var l) == false)
                    return false;
                limit = l;
            }

            int? offset = null;
            if (selectStmt.LimitOffset != null)
            {
                if (TryExtractOffset(selectStmt, out var o) == false)
                    return false;
                offset = o;
            }

            if (TryExtractSortClauseToOrderBy(selectStmt, out var orderByCols, out var orderByDescFlags) == false)
                return false;

            List<string> groupByCols = null;
            List<Aggregate> aggregates = null;
            if (TryFindInnerGroupedSelect(selectStmt, out var groupedSelect))
            {
                if (TryExtractGroupByColumns(groupedSelect, out groupByCols) == false)
                    return false;

                TryExtractAggregates(groupedSelect, out aggregates);

                if (orderByCols.Count > 0)
                {
                    for (int i = 0; i < orderByCols.Count; i++)
                    {
                        if (TryResolveAliasThroughWrappers(selectStmt, orderByCols[i], out var resolved))
                            orderByCols[i] = resolved;
                    }
                }
            }

            // Collect any WHERE clauses that PowerBI planted inside intermediate wrapper levels
            // e.g. user filters placed underneath the null-ordering CASE helpers but above the
            // distinct-grouping. The outer (top-level) WHERE is already carried via OuterWhereClause;
            // we only collect interior WHEREs here so the caller knows to translate them with the
            // right wrapper alias and AND-merge them into the final RQL.
            var intermediateWheres = CollectIntermediateWheres(selectStmt);

            wrapper = new NormalizedWrapper(
                OuterProjectedColumns: outerCols,
                GroupByColumns: groupByCols,
                OrderByColumns: orderByCols,
                OrderByDescFlags: orderByDescFlags,
                OuterWhereClause: selectStmt.WhereClause,
                Limit: limit,
                Offset: offset,
                Aggregates: aggregates,
                IntermediateWheres: intermediateWheres);
            return true;
        }

        // Walks every nested PowerBI-wrapper level *below* the outermost SELECT and collects any
        // WHERE clauses found along the way, tagged with the wrapper alias that scopes the WHERE's
        // column references. Stops descending when it hits a level whose FROM isn't a recognized
        // PowerBI wrapper subselect (RangeVar / non-wrapper alias / multi-source FROM). The outermost
        // SELECT is excluded - its WHERE is already on OuterWhereClause and uses the outermost
        // wrapper alias which the caller handles separately.
        private static List<IntermediateWhere> CollectIntermediateWheres(SelectStmt outerSelectStmt)
        {
            var result = new List<IntermediateWhere>();
            if (outerSelectStmt?.FromClause is not { Count: 1 })
                return result;

            var rss = outerSelectStmt.FromClause[0]?.RangeSubselect;
            if (rss == null)
                return result;

            var current = rss.Subquery?.SelectStmt;
            while (current != null)
            {
                if (current.FromClause is not { Count: 1 } currentFrom)
                    break;

                var nextRss = currentFrom[0]?.RangeSubselect;
                if (nextRss == null)
                    break;

                var nextAlias = nextRss.Alias?.Aliasname;
                if (PgSqlAstHelpers.IsPowerBiWrapperAlias(nextAlias) == false)
                    break;

                // A WHERE clause at THIS level references columns via `nextAlias.<column>`
                // because nextAlias is the alias of THIS level's FROM-subquery (the source the
                // WHERE filters).
                if (current.WhereClause != null)
                    result.Add(new IntermediateWhere(current.WhereClause, nextAlias));

                current = nextRss.Subquery?.SelectStmt;
            }

            return result;
        }

        private static bool TryExtractProjectedColumnsFromAnyWrapperLevel(SelectStmt s, out List<string> cols)
        {
            cols = null;
            var current = s;
            while (current != null)
            {
                if (TryExtractOuterProjectedColumns(current, out cols))
                    return true;

                if (TryExtractSimpleProjectedColumns(current, out cols))
                    return true;

                if (current.FromClause is not { Count: 1 } from)
                    return false;

                current = from[0]?.RangeSubselect?.Subquery?.SelectStmt;
            }

            return false;
        }

        private static bool TryExtractSimpleProjectedColumns(SelectStmt s, out List<string> cols)
        {
            cols = null;

            if (s?.TargetList == null || s.TargetList.Count == 0)
                return false;

            cols = new List<string>(capacity: s.TargetList.Count);
            foreach (var t in s.TargetList)
            {
                var rt = t?.ResTarget;
                if (rt == null)
                    return false;

                var colRef = rt.Val?.ColumnRef;
                if (colRef == null)
                {
                    // Accept PowerBI null-order CASE helpers; reject everything else.
                    if (IsPowerBIOrderHelperAlias(rt.Name) && rt.Val?.CaseExpr != null)
                        continue;
                    return false;
                }

                string colName;
                if (colRef.Fields is { Count: > 1 })
                {
                    if (IsPowerBIOrderHelperAlias(rt.Name))
                        continue;

                    colName = TryExtractLastIdentifierSegment(colRef);
                    if (string.IsNullOrWhiteSpace(colName))
                        return false;

                    if (IsPowerBIOrderHelperAlias(colName))
                        continue;

                    cols.Add(colName);
                    continue;
                }

                colName = TryExtractLastIdentifierSegment(colRef);
                if (string.IsNullOrWhiteSpace(colName))
                    return false;

                if (IsPowerBIOrderHelperAlias(colName))
                    continue;

                cols.Add(colName);
            }

            return cols.Count > 0;
        }

        private static bool TryFindInnerGroupedSelect(SelectStmt outerSelectStmt, out SelectStmt groupedSelect)
        {
            groupedSelect = null;

            if (outerSelectStmt?.FromClause is not { Count: 1 })
                return false;

            var rss = outerSelectStmt.FromClause[0]?.RangeSubselect;
            if (rss == null)
                return false;

            var current = rss.Subquery?.SelectStmt;
            while (current != null)
            {
                if (current.GroupClause is { Count: > 0 })
                {
                    groupedSelect = current;
                    return true;
                }

                if (current.FromClause is not { Count: 1 } currentFrom)
                    return false;

                var next = currentFrom[0]?.RangeSubselect?.Subquery?.SelectStmt;
                if (next == null)
                    return false;

                current = next;
            }

            return false;
        }

        public static bool TryExtractAggregates(SelectStmt groupedSelect, out List<Aggregate> aggregates)
        {
            aggregates = null;

            if (groupedSelect?.TargetList == null || groupedSelect.TargetList.Count == 0)
                return false;

            var list = new List<Aggregate>();
            foreach (var t in groupedSelect.TargetList)
            {
                var rt = t?.ResTarget;
                if (rt?.Val == null)
                    continue;

                var func = PgSqlAstHelpers.UnwrapThroughHarmlessNodes(rt.Val, static n => n.FuncCall);
                if (func == null)
                    continue;

                var name = func.Funcname is { Count: > 0 }
                    ? func.Funcname[^1].String?.Sval // last segment: schema-qualified calls are [schema, name]
                    : null;

                if (string.IsNullOrWhiteSpace(name))
                    continue;

                if (func.Args is not { Count: 1 } args)
                    continue;

                var arg = PgSqlAstHelpers.UnwrapThroughHarmlessNodes(args[0], static n => n.ColumnRef);
                if (arg?.Fields is not { Count: > 0 } fields)
                    continue;

                var fieldName = fields[^1].String?.Sval;
                var outputColumn = rt.Name;
                if (string.IsNullOrWhiteSpace(fieldName) || string.IsNullOrWhiteSpace(outputColumn))
                    continue;

                list.Add(new Aggregate(FunctionName: name, FieldName: fieldName, OutputColumn: outputColumn));
            }

            if (list.Count == 0)
                return false;

            aggregates = list;
            return true;
        }

        private static bool TryExtractOuterProjectedColumns(SelectStmt selectStmt, out List<string> cols)
        {
            cols = null;

            if (selectStmt?.TargetList == null || selectStmt.TargetList.Count == 0)
                return false;

            cols = new List<string>(capacity: selectStmt.TargetList.Count);
            foreach (var t in selectStmt.TargetList)
            {
                var resTarget = t?.ResTarget;
                if (resTarget == null)
                    return false;

                var colRef = resTarget.Val?.ColumnRef;
                if (colRef == null)
                {
                    // Accept PowerBI null-order CASE helpers; reject everything else.
                    if (IsPowerBIOrderHelperAlias(resTarget.Name) && resTarget.Val?.CaseExpr != null)
                        continue;
                    return false;
                }

                if (TryExtractOuterUnderscoreQualifiedColumn(colRef, out var colName) == false)
                {
                    if (IsPowerBIOrderHelperAlias(resTarget.Name))
                        continue;
                    return false;
                }

                if (IsPowerBIOrderHelperAlias(colName))
                    continue;

                cols.Add(colName);
            }

            return cols.Count > 0;
        }

        // PowerBI order-helper aliases - "t<N>_0" (null-order CASE helpers) and "o<N>" (order passthroughs).
        internal static bool IsPowerBIOrderHelperAlias(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            // t<N>_0
            if (name.Length >= 4 && (name[0] == 't' || name[0] == 'T'))
            {
                var u = name.IndexOf('_');
                if (u >= 2 && u < name.Length - 1 &&
                    name.AsSpan(u).Equals("_0", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(name.AsSpan(1, u - 1), out _))
                    return true;
            }

            // o<N>
            if (name.Length >= 2 && (name[0] == 'o' || name[0] == 'O') &&
                int.TryParse(name.AsSpan(1), out _))
                return true;

            return false;
        }

        private static bool TryExtractGroupByColumns(SelectStmt selectStmt, out List<string> cols)
        {
            cols = null;

            if (selectStmt?.GroupClause == null || selectStmt.GroupClause.Count == 0)
                return false;

            cols = new List<string>(capacity: selectStmt.GroupClause.Count);
            foreach (var node in selectStmt.GroupClause)
            {
                // Unwrap ::text / RelabelType casts PowerBI sometimes adds.
                var colRef = PgSqlAstHelpers.UnwrapThroughHarmlessNodes(node, static n => n.ColumnRef);
                if (colRef == null)
                    return false;

                var colName = TryExtractLastIdentifierSegment(colRef);
                if (string.IsNullOrWhiteSpace(colName))
                    return false;

                cols.Add(colName);
            }

            return true;
        }

        private static bool TryResolveAliasThroughWrappers(SelectStmt outer, string aliasName, out string resolved)
        {
            resolved = aliasName;

            if (outer == null || string.IsNullOrWhiteSpace(aliasName))
                return false;

            var current = outer;
            var currentName = aliasName;

            while (current != null)
            {
                var target = FindTargetByAlias(current, currentName);
                if (target == null)
                {
                    current = SingleWrapperChild(current);
                    continue;
                }

                var val = target.Val;
                if (val?.ColumnRef != null)
                {
                    var next = TryExtractLastIdentifierSegment(val.ColumnRef);
                    if (string.IsNullOrWhiteSpace(next))
                        return false;
                    currentName = next;
                    resolved = next;
                    current = SingleWrapperChild(current);
                    continue;
                }

                if (val?.CaseExpr != null && TryExtractNullOrderHelperGuardedColumn(val.CaseExpr, out var guarded))
                {
                    currentName = guarded;
                    resolved = guarded;
                    current = SingleWrapperChild(current);
                    continue;
                }

                return false;
            }

            return true;
        }

        private static ResTarget FindTargetByAlias(SelectStmt s, string name)
        {
            if (s?.TargetList == null)
                return null;

            foreach (var t in s.TargetList)
            {
                var rt = t?.ResTarget;
                if (rt == null)
                    continue;

                var rtName = rt.Name;
                if (string.IsNullOrWhiteSpace(rtName))
                    rtName = rt.Val?.ColumnRef is { Fields.Count: > 0 } cr ? cr.Fields[^1]?.String?.Sval : null;

                if (string.Equals(rtName, name, StringComparison.OrdinalIgnoreCase))
                    return rt;
            }

            return null;
        }

        private static SelectStmt SingleWrapperChild(SelectStmt s) =>
            s?.FromClause is { Count: 1 } from
                ? from[0]?.RangeSubselect?.Subquery?.SelectStmt
                : null;

        // Matches `case when X is [not] null then X else <literal> end` and returns X's last segment.
        internal static bool TryExtractNullOrderHelperGuardedColumn(CaseExpr caseExpr, out string columnName)
        {
            columnName = null;

            if (caseExpr?.Args is not { Count: 1 } args)
                return false;

            var when = args[0]?.CaseWhen;
            if (when?.Expr?.NullTest?.Arg?.ColumnRef is not { } guardCol)
                return false;

            if (when.Result?.ColumnRef == null)
                return false;

            columnName = TryExtractLastIdentifierSegment(guardCol);
            return string.IsNullOrWhiteSpace(columnName) == false;
        }

        internal static bool TryExtractLimit(SelectStmt selectStmt, out int limit)
        {
            return PgSqlAstHelpers.TryReadNonNegativeIntConst(selectStmt?.LimitCount, out limit);
        }

        internal static bool TryExtractOffset(SelectStmt selectStmt, out int offset)
        {
            return PgSqlAstHelpers.TryReadNonNegativeIntConst(selectStmt?.LimitOffset, out offset);
        }

        // Shared by recognizer + emitter. Returns the last identifier segment of a ColumnRef
        // verbatim - both `id` (PG-idiomatic) and `id()` (legacy RQL function-call form) pass
        // through unchanged so the projection key the client requested is preserved in the
        // emitted RQL. Downstream comparisons against synthetic-column names go through
        // PgSyntheticColumns.IsSyntheticColumn so either form matches.
        internal static string TryExtractLastIdentifierSegment(ColumnRef colRef)
        {
            if (colRef?.Fields == null || colRef.Fields.Count == 0)
                return null;

            var last = colRef.Fields[^1];
            var name = last?.String?.Sval;
            return string.IsNullOrWhiteSpace(name) ? null : name;
        }

        internal static bool TryExtractOuterUnderscoreQualifiedColumn(ColumnRef colRef, out string colName)
        {
            colName = null;

            if (colRef?.Fields == null || colRef.Fields.Count < 2)
                return false;

            var first = colRef.Fields[0]?.String?.Sval;
            if (string.Equals(first, "_", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            colName = TryExtractLastIdentifierSegment(colRef);
            return string.IsNullOrWhiteSpace(colName) == false;
        }

        // Accepts outer "_"-qualified (wrapped shape) or unqualified (flat shape). Empty SortClause -> true.
        private static bool TryExtractSortClauseToOrderBy(SelectStmt selectStmt, out List<string> cols, out List<bool> descFlags)
        {
            cols = new List<string>();
            descFlags = new List<bool>();

            if (selectStmt?.SortClause is not { Count: > 0 } sort)
                return true;

            cols = new List<string>(capacity: sort.Count);
            descFlags = new List<bool>(capacity: sort.Count);

            foreach (var sortNode in sort)
            {
                var sortBy = sortNode?.SortBy;
                if (sortBy == null)
                    return false;

                var colRef = PgSqlAstHelpers.UnwrapThroughHarmlessNodes(sortBy.Node, static n => n.ColumnRef);
                if (colRef == null)
                    return false;

                if (TryExtractOuterUnderscoreQualifiedColumn(colRef, out var colName) == false)
                {
                    colName = TryExtractLastIdentifierSegment(colRef);
                    if (string.IsNullOrWhiteSpace(colName))
                        return false;
                }

                cols.Add(colName);
                descFlags.Add(sortBy.SortbyDir == SortByDir.SortbyDesc);
            }

            return true;
        }
    }
}
