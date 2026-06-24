using System;
using System.Collections.Generic;
using System.Globalization;
using PgSqlParser;
using Sparrow;
using Raven.Server.Integrations.PostgreSQL.Translation;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public static class PowerBIFetchQuery
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(PowerBIFetchQuery));

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            if (TryParseSimpleTableFetchViaAst(queryText, parametersDataTypes, documentDatabase, out pgQuery))
                return true;

            if (TryParseWrappedRqlFetchViaAst(queryText, parametersDataTypes, documentDatabase, out pgQuery))
                return true;

            pgQuery = null;
            return false;
        }

        private static bool TryParseWrappedRqlFetchViaAst(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            pgQuery = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            try
            {
                var inner = PowerBIInnerRqlExtractor.TryExtractAndResolve(queryText);
                if (inner == null)
                    return false;

                var selectStmt = inner.SanitizedSelectStmt;
                if (selectStmt == null)
                    return false;

                int? limit = null;
                if (selectStmt.LimitCount != null)
                {
                    if (PgSqlAstHelpers.TryReadNonNegativeIntConst(selectStmt.LimitCount, out var l))
                        limit = l;
                }

                var query = inner.ResolvedQuery;

                Dictionary<string, ReplaceColumnValue> allReplaces = null;
                List<Dictionary<string, ReplaceColumnValue>> wrapperReplaces = null;
                List<(Node WhereClause, string WrapperAlias)> deferredWheres = null;

                var currentSelect = selectStmt;
                var isOuterMost = true;

                while (true)
                {
                    if (IsWrapperRangeSubselectSelect(currentSelect, out var wrapperAlias, out var nextSelect) == false)
                    {
                        if (isOuterMost)
                            return false;

                        break;
                    }

                    if (TryExtractPowerBiReplaceColumns(currentSelect, wrapperAlias, out var levelReplaces) == false)
                        return false;

                    if (levelReplaces != null)
                    {
                        wrapperReplaces ??= new List<Dictionary<string, ReplaceColumnValue>>();
                        wrapperReplaces.Add(levelReplaces);
                    }

                    // Defer WHERE application - we need the innermost aggregate-output aliases
                    // first to know which outer WHEREs to drop (PowerBI's post-aggregate null
                    // guards like `not "_"."a0" is null` would otherwise translate against the
                    // pre-aggregation inner query and fail).
                    if (currentSelect.WhereClause != null)
                    {
                        deferredWheres ??= new List<(Node, string)>();
                        deferredWheres.Add((currentSelect.WhereClause, wrapperAlias));
                    }

                    if (nextSelect == null)
                        break;

                    currentSelect = nextSelect;
                    isOuterMost = false;
                }

                if (wrapperReplaces != null)
                {
                    for (int i = wrapperReplaces.Count - 1; i >= 0; i--)
                    {
                        allReplaces ??= new Dictionary<string, ReplaceColumnValue>(StringComparer.OrdinalIgnoreCase);
                        foreach (var kvp in wrapperReplaces[i])
                            allReplaces[kvp.Key] = kvp.Value;
                    }
                }

                // currentSelect's TargetList is sanitized (real inner replaced with `select 1`
                // so pgsqlparser can read the wrapper). Re-parse the raw inner text to recover
                // its aggregate aliases. RQL inner fails parsing and yields an empty set -
                // RQL doesn't preserve SQL aliases anyway.
                var aggregateOutputAliases = CollectAggregateAliasesFromInnerSql(inner.InnerText);

                if (deferredWheres != null)
                {
                    foreach (var (whereClause, wrapperAlias) in deferredWheres)
                    {
                        if (PowerBIDirectQuery.IsAggregateOutputNullGuard(whereClause, aggregateOutputAliases))
                            continue; // structural null-guard on an aggregate output - drop it (PowerBI artifact, not a user filter)

                        if (PowerBIDirectQuery.WhereClauseReferencesAnyColumn(whereClause, aggregateOutputAliases))
                            return false; // real measure filter on an aggregate output - can't express post-grouping; fall through

                        if (PowerBIOuterWhereTranslator.TryTranslateWhere(whereClause, wrapperAlias, query.From.Alias, out var whereExpression) == false)
                            return false;

                        query.Where = query.Where == null
                            ? whereExpression
                            : new BinaryExpression(query.Where, whereExpression, OperatorType.And);
                    }
                }

                // PowerBI's row-preview / drill-down queries decorate the outermost SELECT with constant
                // markers like `1 as "c0"` that the inner RQL doesn't produce. Collect them and pass to
                // PowerBIRqlQuery, which appends them as synthetic columns after the json append so the
                // wire-order matches PowerBI's SQL (id, user cols, json, c0).
                var constProjections = TryCollectOuterConstProjections(selectStmt);

                // Carry the outermost ORDER BY onto the resolved query so PowerBI's sort (e.g.
                // "sort by measure": `order by "_"."a0" desc`) isn't dropped.
                ApplyOuterOrderBy(selectStmt, query, aggregateOutputAliases);

                var newRql = query.ToString();

                // RowDescription expectations come from the outermost projection. Use PowerBIRqlQuery
                // only when synthetic id+json are wanted (narrow projections must not pick them up) or
                // when REPLACE() rewrites need its substitution machinery; const projections work on both
                // paths, so a `1 as "c0"` marker alone doesn't force the wide path.
                if (WantsPowerBISyntheticColumns(selectStmt) || allReplaces != null)
                {
                    pgQuery = new PowerBIRqlQuery(newRql, parametersDataTypes, documentDatabase, allReplaces, limit: limit, constProjections: constProjections);
                }
                else
                {
                    pgQuery = new PgSqlTranslatedRqlQuery(newRql, parametersDataTypes, documentDatabase, limit: limit, constProjections: constProjections);
                }
                return true;
            }
            catch (Exception e)
            {
                PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIFetchQuery)}.{nameof(TryParseWrappedRqlFetchViaAst)} rejected query.", e);
                pgQuery = null;
                return false;
            }

            static bool IsWrapperRangeSubselectSelect(SelectStmt s, out string alias, out SelectStmt nextSelect)
            {
                alias = null;
                nextSelect = null;

                if (s.FromClause is not { Count: 1 })
                    return false;

                var fromItem = s.FromClause[0];
                if (fromItem?.RangeSubselect == null)
                    return false;

                if (fromItem.RangeVar != null || fromItem.JoinExpr != null)
                    return false;

                var rss = fromItem.RangeSubselect;
                alias = rss.Alias?.Aliasname;
                if (PgSqlAstHelpers.IsPowerBiWrapperAlias(alias) == false)
                    return false;

                nextSelect = rss.Subquery?.SelectStmt;
                return true;
            }
        }

        // Parses the raw inner text as SQL and collects aggregate-output aliases (`<agg>(...) AS
        // <alias>`). Outer wrapper levels reference these in post-aggregation null guards (e.g.
        // `where not "_"."a0" is null`); those WHEREs must be dropped, since the emitted RQL already
        // encodes the aggregation and the alias isn't a real field. Returns empty for non-SQL inner
        // text (e.g. embedded RQL), which carries no SQL aliases anyway.
        private static HashSet<string> CollectAggregateAliasesFromInnerSql(string innerText)
        {
            var aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(innerText))
                return aliases;

            try
            {
                var parseResult = SqlAstCache.GetOrParse(innerText);
                if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: 1 })
                    return aliases;

                var innerSelect = parseResult.Value.Stmts[0]?.Stmt?.SelectStmt;
                if (innerSelect?.TargetList == null)
                    return aliases;

                foreach (var t in innerSelect.TargetList)
                {
                    var resTarget = t?.ResTarget;
                    var funcCall = resTarget?.Val?.FuncCall;
                    if (funcCall == null)
                        continue;
                    if (string.IsNullOrWhiteSpace(resTarget.Name))
                        continue;

                    // Only real aggregates produce a post-grouping output alias. A scalar-function
                    // alias (e.g. `lower(Name) as ln`) must NOT be treated as one - otherwise its
                    // ORDER BY term gets an `as double` cast (garbage on text) and its WHERE is
                    // misclassified as a measure filter.
                    var funcName = funcCall.Funcname is { Count: > 0 }
                        ? funcCall.Funcname[^1]?.String?.Sval
                        : null;
                    if (IsAggregateFunctionName(funcName) == false)
                        continue;

                    aliases.Add(resTarget.Name);
                }
            }
            catch
            {
                // Inner text isn't SQL (probably RQL or some malformed shape) - leave the
                // alias set empty. Outer WHEREs that reference aggregate aliases via this
                // path won't occur because the alias structure only exists in SQL.
            }

            return aliases;
        }

        private static bool IsAggregateFunctionName(string name) =>
            string.Equals(name, "sum", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "count", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "avg", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "min", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(name, "max", StringComparison.OrdinalIgnoreCase);

        // Applies the outermost SQL ORDER BY to the resolved RQL query. Each `_`-qualified sort
        // column maps to the query's projected output name: aggregate-output aliases are tagged
        // `as double` (RQL sorts a projected alias numerically only with the cast - without it the
        // alias sorts lexically), group keys / plain columns use the implicit (natural) order.
        // Best-effort: an unresolvable sort term leaves ORDER BY unset (rows come back unsorted)
        // rather than failing the whole query.
        private static void ApplyOuterOrderBy(SelectStmt outermost, Documents.Queries.AST.Query query, HashSet<string> aggregateAliases)
        {
            if (outermost?.SortClause is not { Count: > 0 } sortClause)
                return;

            // Don't clobber an ordering the inner RQL already carries.
            if (query.OrderBy is { Count: > 0 })
                return;

            var orderBy = new List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending, NullsOrderingType NullsOrdering)>(sortClause.Count);

            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode?.SortBy;
                if (sortBy == null)
                    return;

                var colRef = PgSqlAstHelpers.UnwrapThroughHarmlessNodes(sortBy.Node, static n => n.ColumnRef);
                if (colRef == null)
                    return;

                if (PowerBIWrapperRecognizer.TryExtractOuterUnderscoreQualifiedColumn(colRef, out var colName) == false)
                {
                    colName = PowerBIWrapperRecognizer.TryExtractLastIdentifierSegment(colRef);
                    if (string.IsNullOrWhiteSpace(colName))
                        return;
                }

                var fieldType = aggregateAliases.Contains(colName)
                    ? OrderByFieldType.Double
                    : OrderByFieldType.Implicit;
                var ascending = sortBy.SortbyDir != SortByDir.SortbyDesc;

                orderBy.Add((new FieldExpression(new List<StringSegment> { new StringSegment(colName) }), fieldType, ascending, NullsOrderingType.Implicit));
            }

            query.OrderBy = orderBy;
        }

        // Scans the outermost SELECT's TargetList for `<literal> as <alias>` projections -
        // typically `1 as "c0"` from PowerBI's row-preview shape - and packages them as
        // ConstProjection descriptors. Per-literal typing happens in TryBuildConstProjection.
        private static List<ConstProjection> TryCollectOuterConstProjections(SelectStmt outermost)
        {
            var targets = outermost?.TargetList;
            if (targets == null || targets.Count == 0)
                return null;

            List<ConstProjection> constProjections = null;

            foreach (var t in targets)
            {
                var resTarget = t?.ResTarget;
                var aConst = resTarget?.Val?.AConst;
                if (aConst == null)
                    continue;

                if (TryBuildConstProjection(aConst, resTarget.Name, out var cp) == false)
                    continue;

                constProjections ??= new List<ConstProjection>();
                constProjections.Add(cp);
            }

            return constProjections;
        }

        // Maps the three concrete AConst kinds plus Boolval and SQL NULL to typed wire values.
        // Integer literals are int4 - that's PG's default inference for an unadorned `1` token
        // and what PowerBI's OLE DB type-mapping expects. Float literals are float8 (numeric
        // promotion at parse time is rare for what PowerBI emits). Strings are text. Booleans
        // are bool. All-null components emit a NULL value in the synthetic column.
        private static bool TryBuildConstProjection(A_Const c, string alias, out ConstProjection projection)
        {
            projection = null;
            if (c == null || string.IsNullOrWhiteSpace(alias))
                return false;

            if (c.Ival != null)
            {
                // PG types `1` as int4 - narrow long to int. Out-of-range silently wraps,
                // which is fine for PowerBI's row-preview markers (always small positive ints).
                projection = new ConstProjection(alias, PgInt4.Default, (int)c.Ival.Ival);
                return true;
            }

            if (c.Fval != null && string.IsNullOrEmpty(c.Fval.Fval) == false &&
                double.TryParse(c.Fval.Fval, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
            {
                projection = new ConstProjection(alias, PgFloat8.Default, dv);
                return true;
            }

            if (c.Sval != null && c.Sval.Sval != null)
            {
                projection = new ConstProjection(alias, PgText.Default, c.Sval.Sval);
                return true;
            }

            if (c.Boolval != null)
            {
                projection = new ConstProjection(alias, PgBool.Default, c.Boolval.Boolval);
                return true;
            }

            // All-null components -> SQL NULL. The synthetic column will encode as wire NULL
            // (no bytes, length prefix = -1) regardless of declared type.
            if (c.Ival == null && c.Fval == null && c.Sval == null && c.Boolval == null)
            {
                projection = new ConstProjection(alias, PgText.Default, null);
                return true;
            }

            return false;
        }

        private static bool TryExtractPowerBiReplaceColumns(SelectStmt selectStmt, string outerAlias, out Dictionary<string, ReplaceColumnValue> replaces)
        {
            replaces = null;

            var targets = selectStmt?.TargetList;
            if (targets == null || targets.Count == 0)
                return true;

            foreach (var t in targets)
            {
                var resTarget = t?.ResTarget;
                var funcCall = resTarget?.Val?.FuncCall;
                if (funcCall == null)
                    continue;

                var funcName = funcCall.Funcname is { Count: > 0 }
                    ? funcCall.Funcname[^1].String?.Sval // last segment: schema-qualified calls are [schema, name]
                    : null;

                if (string.Equals(funcName, "replace", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                var dstColumn = resTarget.Name;
                if (string.IsNullOrWhiteSpace(dstColumn))
                    return false;

                if (funcCall.Args is not { Count: 3 })
                    return false;

                var arg0 = funcCall.Args[0];
                if (arg0?.ColumnRef?.Fields is not { Count: 2 } fields)
                    return false;

                var arg0Alias = fields[0].String?.Sval;
                if (string.IsNullOrWhiteSpace(arg0Alias) || string.Equals(arg0Alias, outerAlias, StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                var srcColumn = fields[1].String?.Sval;
                if (string.IsNullOrWhiteSpace(srcColumn))
                    return false;

                var oldValue = funcCall.Args[1]?.AConst?.Sval?.Sval;
                var newValue = funcCall.Args[2]?.AConst?.Sval?.Sval;

                if (oldValue == null || newValue == null)
                    return false;

                replaces ??= new Dictionary<string, ReplaceColumnValue>(StringComparer.OrdinalIgnoreCase);
                replaces[srcColumn] = new ReplaceColumnValue
                {
                    SrcColumnName = srcColumn,
                    DstColumnName = dstColumn,
                    OldValue = oldValue,
                    NewValue = newValue
                };
            }

            return true;
        }

        private static bool TryParseSimpleTableFetchViaAst(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            pgQuery = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            if (TryGetSimplePublicRangeVarSelect(queryText, out var selectStmt) == false)
                return false;

            if (PgSqlToRqlTranslator.TryParse(queryText, parametersDataTypes, documentDatabase, out var rql) == false)
                return false;

            // Route to PowerBIRqlQuery only when the shape wants the synthetic id/json columns
            // (see WantsPowerBISyntheticColumns); otherwise PgSqlTranslatedRqlQuery keeps the
            // RowDescription column-for-column.
            var wantsSyntheticColumns = WantsPowerBISyntheticColumns(selectStmt);

            pgQuery = wantsSyntheticColumns
                ? new PowerBIRqlQuery(rql, parametersDataTypes, documentDatabase, replaces: null, limit: null)
                : new PgSqlTranslatedRqlQuery(rql, parametersDataTypes, documentDatabase);
            return true;
        }

        // True for shapes where PowerBI expects id and json in the RowDescription - either
        // `SELECT *` (implicit "all columns") or an explicit projection that names id / json.
        // False for narrow projections (user columns only, DISTINCT, GROUP BY) where PowerBI
        // cares only about the exact columns it asked for.
        private static bool WantsPowerBISyntheticColumns(SelectStmt selectStmt)
        {
            var targets = selectStmt.TargetList;
            if (targets == null || targets.Count == 0)
                return true; // No target list -> SELECT *-equivalent -> wants everything.

            // pgsqlparser models `SELECT *` as a single ColumnRef whose first (and only) field
            // is an A_Star node. Match that shape so we don't strip synthetics for the implicit
            // wildcard.
            if (targets.Count == 1)
            {
                var only = targets[0]?.ResTarget?.Val;
                var fields = only?.ColumnRef?.Fields;
                if (fields is { Count: 1 } && fields[0]?.AStar != null)
                    return true;
            }

            // Explicit projection: scan each target's column ref for `id` or `json` as the
            // final path segment. PowerBI's standard fetch shape always names them - anything
            // else (user columns only / slicer-distinct / aggregate output) deliberately omits
            // them, and we should mirror that to keep the RowDescription matched.
            foreach (var t in targets)
            {
                var col = t?.ResTarget?.Val?.ColumnRef;
                var colFields = col?.Fields;
                if (colFields == null || colFields.Count == 0)
                    continue;

                var last = colFields[colFields.Count - 1]?.String?.Sval;
                if (string.IsNullOrEmpty(last))
                    continue;

                // Accept both the new PG-idiomatic forms (`id`, `json`) and the legacy
                // parenthesised forms (`id()`, `json()`) that older PowerBI metadata caches
                // still send. Either signals "this is the PowerBI fetch shape - keep synthetics".
                if (PgSyntheticColumns.IsSyntheticColumn(last))
                    return true;
            }

            return false;
        }

        private static bool TryGetSimplePublicRangeVarSelect(string sql, out SelectStmt selectStmt)
        {
            selectStmt = null;

            var parseResult = SqlAstCache.GetOrParse(sql);
            if (parseResult.IsSuccess == false || parseResult.Value == null)
                return false;

            if (parseResult.Value.Stmts == null || parseResult.Value.Stmts.Count != 1)
                return false;

            var stmt = parseResult.Value.Stmts[0];
            var select = stmt?.Stmt?.SelectStmt;
            if (select == null)
                return false;

            if (select.FromClause is not { Count: 1 })
                return false;

            var rangeVar = select.FromClause[0]?.RangeVar;
            if (rangeVar == null)
                return false;

            if (string.Equals(rangeVar.Schemaname, "public", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (string.IsNullOrWhiteSpace(rangeVar.Relname))
                return false;

            selectStmt = select;
            return true;
        }

    }
}
