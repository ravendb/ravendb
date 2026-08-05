using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using PgSqlParser;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    internal static class PgSqlToRqlTranslator
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(PgSqlToRqlTranslator));

        // Architectural note: the translator swallows NotSupportedException inside TryParse and
        // returns false, letting PgQuery.CreateInstance fall through to UnhandledQueryDiagnoser
        // for user-facing classification. The re-parse cost in the diagnoser is amortized via
        // SqlAstCache (same dispatch chain → cache hit). Throws below use PgTranslationException
        // (a NotSupportedException subclass) so the failure carries a machine-actionable category;
        // ad-hoc throws elsewhere still use NotSupportedException(string) until migrated.
        private static PgTranslationException UnsupportedSelectAggregate() =>
            new(TranslationFailureCategory.Aggregate, "Unsupported SELECT aggregate");
        // DISTINCT / FILTER / OVER each change which values the aggregate sees, and RQL's grouped
        // select can express none of them — emitting the plain aggregate returns the whole group's
        // value instead, with no error.
        private static PgTranslationException UnsupportedAggregateModifier(string modifier) =>
            new(TranslationFailureCategory.Aggregate, $"Aggregate {modifier} is not supported");
        // RQL's count() is the group's row count and ignores its argument, so it cannot express
        // SQL's count(<column>) (which counts non-null values only).
        private static PgTranslationException UnsupportedCountArgument() =>
            new(TranslationFailureCategory.Aggregate, "count(<column>) is not supported");
        // Scalar aggregate (all-aggregate SELECT with no GROUP BY) and avg() have no RQL form —
        // bail so PgQuery falls through to UnhandledQueryDiagnoser for a user-facing explanation
        // instead of emitting RQL the engine rejects at execution time with an opaque error.
        private static PgTranslationException ScalarAggregateWithoutGroupBy() =>
            new(TranslationFailureCategory.Aggregate, "Scalar aggregate without GROUP BY is not supported");
        // RavenDB's map-reduce 'where' runs POST-reduction, so SQL HAVING and a pre-aggregation WHERE
        // on a non-grouped field both silently change the aggregates if translated naively. Reject so
        // PgQuery falls through to UnhandledQueryDiagnoser instead of returning wrong numbers.
        private static PgTranslationException UnsupportedGroupByHaving() =>
            new(TranslationFailureCategory.GroupBy, "HAVING is not supported");
        private static PgTranslationException UnsupportedGroupByFilter() =>
            new(TranslationFailureCategory.GroupBy, "WHERE on a non-grouped field in a GROUP BY query is not supported");
        private static PgTranslationException UnsupportedSelectProjection() =>
            new(TranslationFailureCategory.SelectProjection, "Unsupported SELECT projection");
        private static PgTranslationException MixedAggregateAndNonAggregate() =>
            new(TranslationFailureCategory.MixedAggregateAndNonAggregate, "Mixing aggregates and non-aggregated columns is not supported yet.");
        private static PgTranslationException UnsupportedDistinct() =>
            new(TranslationFailureCategory.Distinct, "Unsupported DISTINCT");
        private static PgTranslationException UnsupportedGroupBy() =>
            new(TranslationFailureCategory.GroupBy, "Unsupported GROUP BY");
        private static PgTranslationException UnsupportedOrderByForGroupBy() =>
            new(TranslationFailureCategory.OrderBy, "Unsupported ORDER BY for GROUP BY");
        private static PgTranslationException UnsupportedJoin() =>
            new(TranslationFailureCategory.Join, "Unsupported JOIN shape");

        public static bool TryParse(string sql, int[] parameterTypes, out string rql)
            => TryParse(sql, parameterTypes, documentDatabase: null, out rql, out _);

        public static bool TryParse(string sql, int[] parameterTypes, DocumentDatabase documentDatabase, out string rql)
            => TryParse(sql, parameterTypes, documentDatabase, out rql, out _);

        // The documentDatabase overload lets the ORDER BY translator infer numeric vs string
        // ordering by sampling the collection's first document. Without it, every ORDER BY falls
        // back to PG's default (alphabetic), which produces "10 < 9" results for numeric fields.
        //
        // hasExplicitProjection tells the caller whether the SQL had a real projection (e.g.
        // `SELECT a, b FROM t`) or a wildcard (`SELECT * FROM t`). The caller uses it to decide
        // whether to suppress RqlQuery's auto-included id/json columns - if the user
        // explicitly listed columns, they shouldn't get magic extras tacked on.
        public static bool TryParse(string sql, int[] parameterTypes, DocumentDatabase documentDatabase, out string rql, out bool hasExplicitProjection)
        {
            rql = null;
            hasExplicitProjection = false;

            if (Logger.IsDebugEnabled)
                Logger.Debug($"{nameof(PgSqlToRqlTranslator)}.{nameof(TryParse)} invoked with SQL: {sql}");

            try
            {
                var parseResult = SqlAstCache.GetOrParse(sql);

                if (parseResult.IsSuccess == false || parseResult.Value == null)
                {
                    if (Logger.IsDebugEnabled)
                    {
                        var parseError = parseResult.Error;
                        if (parseError != null)
                            Logger.Debug($"{nameof(PgSqlToRqlTranslator)} parse error. Message: {parseError.Message}. SQL: {sql}");
                        else
                            Logger.Debug($"{nameof(PgSqlToRqlTranslator)} parse error. SQL: {sql}");
                    }

                    return LogFailure("parse error");
                }

                if (parseResult.Value.Stmts == null || parseResult.Value.Stmts.Count == 0)
                    throw new NotSupportedException("No statements found in query.");

                var stmt = parseResult.Value.Stmts[0];
                if (stmt?.Stmt?.SelectStmt == null)
                    throw new NotSupportedException("Only SELECT statements are supported.");

                hasExplicitProjection = HasExplicitProjection(stmt.Stmt.SelectStmt);
                rql = TranslateSelectStatement(stmt.Stmt.SelectStmt, documentDatabase);
                return LogSuccess(sql, rql);
            }
            catch (NotSupportedException ex)
            {
                // Intentional swallow (see architectural note above). Log the failure category so we
                // can correlate translator misses with diagnoser outcomes in the wild.
                rql = null;
                var category = (ex as PgTranslationException)?.Category ?? TranslationFailureCategory.Other;
                var reason = string.IsNullOrWhiteSpace(ex.Message)
                    ? $"unsupported query shape (category: {category})"
                    : $"unsupported query shape (category: {category}): {ex.Message}";
                return LogFailure(reason);
            }
        }

        // `SELECT count(*) FROM t [WHERE ...]` is the one scalar aggregate RQL answers exactly: a count
        // query (page size 0) drains the filtered enumerator and reports TotalResults. Kept out of
        // TryParse because its other callers (PowerBI) discard the count shape and must keep rejecting it.
        public static bool TryParseScalarCount(string sql, DocumentDatabase documentDatabase, out string rql, out string[] columnNames)
        {
            rql = null;
            columnNames = null;

            try
            {
                var parseResult = SqlAstCache.GetOrParse(sql);
                if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: > 0 } stmts)
                    return false;

                if (stmts[0]?.Stmt?.SelectStmt is not { } selectStmt)
                    return false;

                if (TryGetScalarCountColumns(selectStmt, out columnNames) == false)
                    return false;

                rql = TranslateSelectStatement(selectStmt, documentDatabase, asScalarCount: true);
                return LogSuccess(sql, rql);
            }
            catch (NotSupportedException)
            {
                rql = null;
                columnNames = null;
                return false;
            }
        }

        // Only count(*) qualifies: count(x) counts non-null values and count(distinct x) needs dedup,
        // neither of which TotalResults reports. Everything else - other aggregates, a bare column,
        // DISTINCT, GROUP BY - keeps falling through to ScalarAggregateWithoutGroupBy.
        private static bool TryGetScalarCountColumns(SelectStmt selectStmt, out string[] columnNames)
        {
            columnNames = null;

            if (selectStmt.GroupClause is { Count: > 0 } ||
                selectStmt.DistinctClause is { Count: > 0 } ||
                selectStmt.SortClause is { Count: > 0 } ||
                selectStmt.HavingClause != null ||
                selectStmt.WithClause != null ||
                selectStmt.LimitOffset != null)
                return false;

            // SQL `LIMIT 0` returns no rows at all, which this always-one-row path can't express.
            if (selectStmt.LimitCount != null &&
                (PgSqlAstHelpers.TryReadNonNegativeIntConst(selectStmt.LimitCount, out var limit) == false || limit == 0))
                return false;

            if (selectStmt.FromClause is not [{ RangeVar: not null }])
                return false;

            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            var names = new string[targets.Count];
            for (int i = 0; i < targets.Count; i++)
            {
                var resTarget = targets[i]?.ResTarget;
                if (IsCountStar(resTarget?.Val?.FuncCall) == false)
                    return false;

                names[i] = string.IsNullOrWhiteSpace(resTarget.Name) ? "count" : resTarget.Name;
            }

            columnNames = names;
            return true;
        }

        private static bool IsCountStar(FuncCall func)
        {
            if (func == null || func.AggStar == false || func.AggDistinct || func.AggFilter != null || func.Over != null)
                return false;

            if (func.Args is { Count: > 0 })
                return false;

            // Match the last name segment so PowerBI's `pg_catalog.count(*)` is recognized too.
            var name = func.Funcname is { Count: > 0 } ? func.Funcname[^1]?.String?.Sval : null;
            return string.Equals(name, "count", StringComparison.OrdinalIgnoreCase);
        }

        private static bool LogSuccess(string sql, string rql)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"{nameof(PgSqlToRqlTranslator)} translated SQL to RQL. SQL: {sql}. RQL: {rql}");
            return true;
        }

        private static bool LogFailure(string reason)
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug($"{nameof(PgSqlToRqlTranslator)} did not translate SQL. Reason: {reason}");
            return false;
        }

        private static string TranslateSelectStatement(SelectStmt selectStmt, DocumentDatabase documentDatabase = null, bool asScalarCount = false)
        {
            if (selectStmt.FromClause is [{ JoinExpr: not null }])
                return TranslateSimpleJoin(selectStmt);

            if (selectStmt.FromClause is not [{ RangeVar: { Relname: var relname } rangeVar }])
                throw new NotSupportedException("FROM clause with collection or index name is required");

            var isIndex = string.Equals(rangeVar.Schemaname, "indexes", StringComparison.OrdinalIgnoreCase);

            // The index name is emitted into the RQL `from index '...'` literal unescaped (unlike the
            // collection path, which FromToken escapes). Index names legitimately contain '/', so reject
            // only the characters that would break the literal rather than gating through RqlIdentifier.IsSafe.
            if (isIndex && (relname.Contains('\'') || relname.Contains('\\')))
                throw new NotSupportedException("Unsupported index name");

            var isGroupBy = selectStmt.GroupClause != null && selectStmt.GroupClause.Count > 0;
            var q = new AsyncDocumentQuery<JObject>(session: null, indexName: isIndex ? relname : null, collectionName: isIndex ? null : relname, isGroupBy: isGroupBy);
            var fromAlias = rangeVar.Alias?.Aliasname;

            // Build WHERE clause (must be applied before GROUP BY)
            if (selectStmt.WhereClause != null)
                TranslateWhereClause(q, selectStmt.WhereClause, fromAlias);

            if (isGroupBy)
            {
                ApplyGroupBy(q, selectStmt, fromAlias);
            }
            else if (asScalarCount)
            {
                // The caller runs this as an RQL count query, so neither the projection nor the SQL
                // bounds may be emitted: in SQL they cap the single output row, not the count.
                return BuildRql(q);
            }
            else
            {
                var aliasToField = ApplySelectProjection(q, selectStmt, fromAlias);

                // Build ORDER BY clause. Type-infer sort fields from a doc sample (when the
                // database is available) so numeric fields sort numerically instead of falling
                // back to alphabetic. Indexes (Schemaname = "indexes") are skipped — they don't
                // have a "first document" in the collection-sampling sense.
                if (selectStmt.SortClause != null && selectStmt.SortClause.Count > 0)
                {
                    var sortTypeMap = isIndex ? null : TryBuildSortTypeMap(documentDatabase, relname, selectStmt.SortClause, fromAlias, aliasToField);
                    TranslateOrderBy(q, selectStmt.SortClause, fromAlias, sortTypeMap, aliasToField);
                }
            }

            // Build OFFSET clause. Fail the whole translation rather than silently
            // dropping an OFFSET we can't represent (parameter placeholders, non-integer
            // constants, …) — silently ignoring the bound would return extra rows.
            if (selectStmt.LimitOffset != null)
            {
                var offset = TranslateLimit(selectStmt.LimitOffset);
                if (offset == null)
                    throw new NotSupportedException("Unsupported OFFSET expression (only integer literals are supported)");
                q.Skip(offset.Value);
            }

            // Build LIMIT clause. Same fail-hard policy as OFFSET.
            if (selectStmt.LimitCount != null)
            {
                var limit = TranslateLimit(selectStmt.LimitCount);
                if (limit == null)
                    throw new NotSupportedException("Unsupported LIMIT expression (only integer literals are supported)");
                q.Take(limit.Value);
            }

            return BuildRql(q);
        }

        private static string BuildRql(AsyncDocumentQuery<JObject> q)
        {
            // Prefer the official query text emitted by IndexQuery when possible.
            // Falls back to ToString() which also returns pure RQL.
            var indexQuery = q.GetIndexQuery();

            var queryText = indexQuery?.Query ?? q.ToString();
            var parameters = indexQuery?.QueryParameters;

            if (parameters != null)
                queryText = InlineQueryParameters(queryText, parameters);

            return queryText;
        }

        private static string InlineQueryParameters(string rql, Parameters parameters)
        {
            if (string.IsNullOrWhiteSpace(rql))
                return rql;

            if (parameters == null || parameters.Count == 0)
                return rql;

            var sb = new StringBuilder(rql.Length);
            for (int i = 0; i < rql.Length; i++)
            {
                var ch = rql[i];
                if (ch != '$' || i + 2 >= rql.Length || rql[i + 1] != 'p')
                {
                    sb.Append(ch);
                    continue;
                }

                int start = i;
                i += 2;

                int numStart = i;
                while (i < rql.Length && char.IsDigit(rql[i]))
                    i++;

                if (numStart == i)
                {
                    sb.Append(rql.AsSpan(start, (i - start) + 1));
                    continue;
                }

                var paramName = rql.Substring(start + 1, i - (start + 1)); // p0
                if (parameters.TryGetValue(paramName, out var value))
                {
                    if (value is IEnumerable enumerable and not string)
                        sb.Append(FormatRqlListItems(enumerable));
                    else
                        sb.Append(FormatRqlLiteral(value));
                }
                else
                {
                    sb.Append('$');
                    sb.Append(paramName);
                }
                i--; // compensate for for-loop increment
            }

            return sb.ToString();
        }

        // Carries a $N parameter index through AsyncDocumentQuery's auto-parameterization
        // (TransformValue leaves unknown reference types untouched) so InlineQueryParameters
        // can rewrite the generated $pN token into an RQL parameter reference ($N) that maps to
        // the Bind-time Parameters dict — instead of inlining a literal we don't have yet.
        private sealed record PgBoundParameterReference(int OneBasedIndex);

        private static string FormatRqlLiteral(object value)
        {
            if (value == null)
                return "null";

            // The $N marker (see PgBoundParameterReference) emits an RQL parameter reference, not a literal.
            if (value is PgBoundParameterReference paramRef)
                return "$" + paramRef.OneBasedIndex.ToString(CultureInfo.InvariantCulture);

            if (value is IEnumerable enumerable and not string)
                return FormatRqlList(enumerable);

            if (value is string s)
                return QuoteString(s);

            if (value is bool b)
                return b ? "true" : "false";

            if (value is int or long or short or byte or uint or ulong or ushort or sbyte)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            if (value is float or double or decimal)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            if (value is DateTime dt)
                return QuoteString(dt.ToString("O", CultureInfo.InvariantCulture));

            if (value is DateTimeOffset dto)
                return QuoteString(dto.ToString("O", CultureInfo.InvariantCulture));

            throw new NotSupportedException($"Unsupported query parameter type '{value.GetType()}'.");
        }

        private static string FormatRqlList(IEnumerable values)
        {
            var sb = new StringBuilder();
            sb.Append('(');

            sb.Append(FormatRqlListItems(values));

            sb.Append(')');
            return sb.ToString();
        }

        private static string FormatRqlListItems(IEnumerable values)
        {
            var sb = new StringBuilder();

            bool first = true;
            foreach (var item in values)
            {
                if (first == false)
                    sb.Append(", ");

                sb.Append(FormatRqlLiteral(item));
                first = false;
            }

            return sb.ToString();
        }

        private static string QuoteString(string s)
        {
            if (s == null)
                return "null";

            // RQL's scanner treats backslash as an escape character inside single-quoted strings, so
            // backslashes must be doubled before single quotes are doubled. Otherwise a value containing
            // a backslash is silently decoded as an escape sequence (corrupting the value), and a crafted
            // value can terminate the literal early and inject RQL once the result is re-parsed.
            var escaped = s
                .Replace("\\", "\\\\", StringComparison.Ordinal)
                .Replace("'", "''", StringComparison.Ordinal);

            return "'" + escaped + "'";
        }

        private static bool IsSafeRqlIdentifier(string s) => RqlIdentifier.IsSafe(s);

        private static string TranslateSimpleJoin(SelectStmt selectStmt)
        {
            if (selectStmt.FromClause is not [{ JoinExpr: { } joinExpr }])
                throw new NotSupportedException("FROM clause with join is required");

            if (IsSelectStar(selectStmt.TargetList ?? []) == false)
                throw UnsupportedJoin();

            if (selectStmt.WhereClause != null || selectStmt.GroupClause is { Count: > 0 } || selectStmt.SortClause is { Count: > 0 } ||
                selectStmt.LimitCount != null || selectStmt.LimitOffset != null || selectStmt.DistinctClause is { Count: > 0 })
                throw UnsupportedJoin();

            if (TryExtractSimpleJoinInfo(joinExpr, out var drivingCollection, out var drivingAlias, out var loadPath, out var loadAlias) == false)
                throw UnsupportedJoin();

            // The aliases are reused as JavaScript projection values (`select { alias: alias }`), where a
            // quoted name would become a string literal, so require bare identifiers here.
            if (IsSafeRqlIdentifier(drivingAlias) == false ||
                IsSafeRqlIdentifier(loadPath) == false ||
                IsSafeRqlIdentifier(loadAlias) == false)
                throw UnsupportedJoin();

            return $"from {QuoteString(drivingCollection)} as {drivingAlias} load {drivingAlias}.{loadPath} as {loadAlias} select {{ {drivingAlias}: {drivingAlias}, {loadAlias}: {loadAlias} }}";
        }

        private static bool TryExtractSimpleJoinInfo(
            JoinExpr joinExpr,
            out string drivingCollection,
            out string drivingAlias,
            out string loadPath,
            out string loadAlias)
        {
            drivingCollection = string.Empty;
            drivingAlias = string.Empty;
            loadPath = string.Empty;
            loadAlias = string.Empty;

            if (TryGetRangeVarWithAlias(joinExpr.Larg, out var leftCollection, out var leftAlias) == false ||
                TryGetRangeVarWithAlias(joinExpr.Rarg, out var rightCollection, out var rightAlias) == false)
                return false;

            if (joinExpr.Quals?.AExpr is not { Kind: A_Expr_Kind.AexprOp, Name.Count: > 0 } onExpr)
                return false;

            var operatorName = GetStringValue(onExpr.Name[0]) ?? string.Empty;
            if (string.Equals(operatorName, "=", StringComparison.Ordinal) == false)
                return false;

            if (onExpr.Lexpr?.ColumnRef == null || onExpr.Rexpr?.ColumnRef == null)
                return false;

            var leftRef = ExtractFieldName(onExpr.Lexpr);
            var rightRef = ExtractFieldName(onExpr.Rexpr);
            if (string.IsNullOrWhiteSpace(leftRef) || string.IsNullOrWhiteSpace(rightRef))
                return false;

            if (TryParseQualifiedField(leftRef, out var leftRefAlias, out var leftRefField) == false ||
                TryParseQualifiedField(rightRef, out var rightRefAlias, out var rightRefField) == false)
                return false;

            if (string.Equals(leftRefField, "id", StringComparison.OrdinalIgnoreCase))
            {
                drivingAlias = rightRefAlias;
                loadPath = rightRefField;
                loadAlias = leftRefAlias;
            }
            else if (string.Equals(rightRefField, "id", StringComparison.OrdinalIgnoreCase))
            {
                drivingAlias = leftRefAlias;
                loadPath = leftRefField;
                loadAlias = rightRefAlias;
            }
            else
            {
                return false;
            }

            if (string.Equals(drivingAlias, leftAlias, StringComparison.OrdinalIgnoreCase))
                drivingCollection = leftCollection;
            else if (string.Equals(drivingAlias, rightAlias, StringComparison.OrdinalIgnoreCase))
                drivingCollection = rightCollection;
            else
                return false;

            return true;
        }

        private static bool TryGetRangeVarWithAlias(Node node, out string collection, out string alias)
        {
            collection = string.Empty;
            alias = string.Empty;

            var rv = node.RangeVar;
            if (rv?.Relname == null)
                return false;

            if (string.IsNullOrWhiteSpace(rv.Schemaname) == false)
                return false;

            collection = rv.Relname;
            alias = rv.Alias?.Aliasname ?? rv.Relname;
            return string.IsNullOrWhiteSpace(alias) == false;
        }

        private static bool TryParseQualifiedField(string field, out string alias, out string path)
        {
            alias = string.Empty;
            path = string.Empty;

            var idx = field.IndexOf('.', StringComparison.Ordinal);
            if (idx <= 0 || idx >= field.Length - 1)
                return false;

            alias = field[..idx];
            path = field[(idx + 1)..];
            return string.IsNullOrWhiteSpace(alias) == false && string.IsNullOrWhiteSpace(path) == false;
        }

        private static void ApplyGroupBy(AsyncDocumentQuery<JObject> q, SelectStmt selectStmt, string fromAlias)
        {
            if (selectStmt.GroupClause is not { Count: > 0 })
                throw UnsupportedGroupBy();

            var groupFieldNames = new List<string>(capacity: selectStmt.GroupClause.Count);
            foreach (var groupKeyNode in selectStmt.GroupClause)
            {
                if (groupKeyNode.ColumnRef == null)
                    throw UnsupportedGroupBy();

                var name = ExtractFieldName(groupKeyNode, fromAlias);
                if (string.IsNullOrWhiteSpace(name))
                    throw UnsupportedGroupBy();

                groupFieldNames.Add(name);
            }

            // RavenDB applies a group-by query's WHERE to the REDUCED result (post-aggregation), not
            // the source rows. SQL HAVING is also post-aggregation but the bridge doesn't translate it.
            // Both would silently corrupt the aggregates, so reject them — except a WHERE that touches
            // only GROUP BY keys, which is equivalent whether applied before or after grouping.
            if (selectStmt.HavingClause != null)
                throw UnsupportedGroupByHaving();

            if (selectStmt.WhereClause != null)
            {
                var keySet = new HashSet<string>(groupFieldNames, StringComparer.OrdinalIgnoreCase);
                if (WhereReferencesOnlyFields(selectStmt.WhereClause, keySet, fromAlias) == false)
                    throw UnsupportedGroupByFilter();
            }

            var targets = selectStmt.TargetList;
            if (targets == null || targets.Count == 0)
                throw UnsupportedGroupBy();

            if (IsSelectStar(targets))
                throw UnsupportedGroupBy();

            if (selectStmt.DistinctClause is { Count: > 0 })
                throw UnsupportedGroupBy();

            bool hasAnyAggregate = false;
            foreach (var t in targets)
            {
                if (t.ResTarget?.Val?.FuncCall != null)
                {
                    hasAnyAggregate = true;
                    break;
                }
            }

            // Distinct-rows shape (PowerBI's "fill a slicer/dropdown" probe):
            //     SELECT col1, col2 ... FROM t GROUP BY col1, col2 [LIMIT N]
            // No aggregates — the GROUP BY is being used as a DISTINCT mechanism.
            //
            // Single-column: emit RQL `select distinct <field>` — Raven dedupes the projection.
            // Multi-column: use `group by <fields> select <fields>` instead. RQL's `select
            // distinct <a>, <b>` is NOT a tuple-distinct — it dedupes by first-field semantics
            // and leaves duplicate tuples in the result, which then breaks PowerBI's mashup
            // engine ("more than one row in the index table matching to the current row"). The
            // group-by form is the canonical tuple-dedup in RQL: every (col1, col2) pair becomes
            // a group, and projecting the group keys gives back exactly the distinct tuples.
            if (hasAnyAggregate == false)
            {
                var groupSet = new HashSet<string>(groupFieldNames, StringComparer.OrdinalIgnoreCase);
                var projected = new List<string>(targets.Count);
                foreach (var t in targets)
                {
                    var val = t.ResTarget?.Val;
                    if (val?.ColumnRef == null)
                        throw UnsupportedGroupBy();

                    var field = ExtractFieldName(val, fromAlias);
                    if (string.IsNullOrWhiteSpace(field) || groupSet.Contains(field) == false)
                        throw UnsupportedGroupBy();

                    projected.Add(BuildDistinctRowProjection(field, t.ResTarget?.Name));
                }

                if (groupFieldNames.Count == 1)
                {
                    q.SelectFields<JObject>(projected.ToArray());
                    q.Distinct();
                }
                else
                {
                    // Multi-key tuple-distinct via group-by. SelectFields after a multi-key
                    // GroupBy projects each group's key values back as columns.
                    var firstKey = groupFieldNames[0];
                    var restKeys = groupFieldNames.Count > 1
                        ? groupFieldNames.GetRange(1, groupFieldNames.Count - 1).ToArray()
                        : Array.Empty<string>();
                    q.GroupBy(firstKey, restKeys);
                    q.SelectFields<JObject>(projected.ToArray());
                }
                return;
            }

            // Aggregate path: only single-column GROUP BY is representable in RQL today.
            if (groupFieldNames.Count != 1)
                throw UnsupportedGroupBy();

            var groupFieldName = groupFieldNames[0];

            var projections = new List<string>(capacity: targets.Count);
            var aggregates = new List<GroupByAggregate>(capacity: targets.Count);
            var groupKeyProjected = false;
            foreach (var t in targets)
            {
                var val = t.ResTarget?.Val;
                if (val == null)
                    throw UnsupportedGroupBy();

                if (val.FuncCall == null)
                {
                    projections.Add(BuildProjectionForGroupByTarget(val, groupFieldName, t.ResTarget?.Name, fromAlias));
                    groupKeyProjected = true;
                    continue;
                }

                var aggregate = BuildGroupByAggregate(val.FuncCall, t.ResTarget?.Name, fromAlias);
                aggregates.Add(aggregate);
                projections.Add(aggregate.Projection);
            }

            q.GroupBy(groupFieldName);
            q.SelectFields<JObject>(projections.ToArray());

            if (selectStmt.SortClause != null && selectStmt.SortClause.Count > 0)
                TranslateOrderByForGroupBy(q, selectStmt.SortClause, groupFieldName, groupKeyProjected, aggregates, fromAlias);
        }

        // True iff every column referenced by the WHERE predicate is in <paramref name="allowed"/>.
        // Returns false on any predicate shape we can't confidently decompose, so the caller rejects
        // rather than risk passing a filter that RavenDB would apply post-aggregation.
        private static bool WhereReferencesOnlyFields(Node where, HashSet<string> allowed, string fromAlias)
        {
            var fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (TryCollectWhereFields(where, fields, fromAlias) == false)
                return false;
            foreach (var f in fields)
                if (allowed.Contains(f) == false)
                    return false;
            return true;
        }

        private static bool TryCollectWhereFields(Node node, HashSet<string> fields, string fromAlias)
        {
            if (node == null)
                return true;
            if (node.ColumnRef != null)
            {
                var f = ExtractFieldName(node, fromAlias);
                if (string.IsNullOrWhiteSpace(f))
                    return false;
                fields.Add(f);
                return true;
            }
            if (node.AConst != null)
                return true;
            if (node.AExpr is { } ae)
                return TryCollectWhereFields(ae.Lexpr, fields, fromAlias) && TryCollectWhereFields(ae.Rexpr, fields, fromAlias);
            if (node.BoolExpr is { Args: { } args })
            {
                foreach (var arg in args)
                    if (TryCollectWhereFields(arg, fields, fromAlias) == false)
                        return false;
                return true;
            }
            if (node.NullTest is { } nt)
                return TryCollectWhereFields(nt.Arg, fields, fromAlias);
            if (node.List is { Items: { } items })
            {
                foreach (var it in items)
                    if (TryCollectWhereFields(it, fields, fromAlias) == false)
                        return false;
                return true;
            }
            // FuncCall / SubLink / CaseExpr / … in a group-by WHERE: can't vouch for it → reject.
            return false;
        }

        private static void TranslateOrderByForGroupBy(
            AsyncDocumentQuery<JObject> q,
            Google.Protobuf.Collections.RepeatedField<Node> sortClause,
            string groupFieldName,
            bool groupKeyProjected,
            IReadOnlyList<GroupByAggregate> aggregates,
            string fromAlias = null)
        {
            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode.SortBy;
                if (sortBy == null)
                    continue;

                RejectUnsupportedSortModifiers(sortNode);

                string orderField;
                OrderingType ordering;

                if (sortBy.Node?.ColumnRef != null)
                {
                    var fieldName = ExtractFieldName(sortBy.Node, fromAlias);
                    if (string.IsNullOrWhiteSpace(fieldName))
                        throw UnsupportedOrderByForGroupBy();

                    if (string.Equals(fieldName, groupFieldName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (groupKeyProjected == false)
                            throw UnsupportedOrderByForGroupBy();

                        // RQL sorts by the reduced field, so the key's SELECT alias is irrelevant here.
                        orderField = groupFieldName;
                        ordering = OrderingType.String;
                    }
                    else
                    {
                        var aggregate = FindAggregate(aggregates, a => string.Equals(a.OutputName, fieldName, StringComparison.OrdinalIgnoreCase));
                        (orderField, ordering) = AggregateOrderTarget(aggregate, groupFieldName);
                    }
                }
                else if (sortBy.Node?.FuncCall != null)
                {
                    var expression = BuildAggregateProjectionForGroupByOrderBy(sortBy.Node.FuncCall, fromAlias);
                    var aggregate = FindAggregate(aggregates, a => string.Equals(a.Expression, expression, StringComparison.OrdinalIgnoreCase));
                    (orderField, ordering) = AggregateOrderTarget(aggregate, groupFieldName);
                }
                else
                {
                    throw UnsupportedOrderByForGroupBy();
                }

                if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                    q.OrderByDescending(orderField, ordering);
                else
                    q.OrderBy(orderField, ordering);
            }
        }

        private static GroupByAggregate FindAggregate(IReadOnlyList<GroupByAggregate> aggregates, Func<GroupByAggregate, bool> predicate)
        {
            for (int i = 0; i < aggregates.Count; i++)
            {
                if (predicate(aggregates[i]))
                    return aggregates[i];
            }

            throw UnsupportedOrderByForGroupBy();
        }

        private static (string Field, OrderingType Ordering) AggregateOrderTarget(GroupByAggregate aggregate, string groupFieldName)
        {
            // sum(<group key>) and the key itself resolve to the same RQL field name, so `order by`
            // would silently sort by the key instead of the aggregate.
            if (string.Equals(aggregate.OrderField, groupFieldName, StringComparison.OrdinalIgnoreCase))
                throw UnsupportedOrderByForGroupBy();

            return (aggregate.OrderField, aggregate.Ordering);
        }

        private static IReadOnlyDictionary<string, string> ApplySelectProjection(AsyncDocumentQuery<JObject> q, SelectStmt selectStmt, string fromAlias)
        {
            var targets = selectStmt.TargetList;
            if (targets == null || targets.Count == 0)
                return null;

            if (IsSelectStar(targets))
                return null;

            var isDistinct = selectStmt.DistinctClause is { Count: > 0 };
            var anyAgg = targets.Any(IsAggregateTarget);
            var allAgg = targets.All(IsAggregateTarget);

            if (isDistinct && anyAgg)
                throw UnsupportedDistinct();

            if (anyAgg && !allAgg)
                throw MixedAggregateAndNonAggregate();

            if (allAgg)
            {
                // No GROUP BY here (this method is the non-group-by branch), so every
                // RQL aggregate form — count()/sum()/avg() — is rejected by the query engine
                // ("X may only be used in group by queries"). Bail to the diagnoser.
                throw ScalarAggregateWithoutGroupBy();
            }

            var (projectionFields, projectionAliases, aliasToField) = BuildColumnProjections(targets, fromAlias);
            if (isDistinct && projectionFields.Length != 1)
                throw UnsupportedDistinct();

            if (projectionFields.Length == 0)
                return null;

            // Distinct Fields vs Projections preserves `1 as "c0"`-style aliases (see BuildColumnProjections).
            q.SelectFields<JObject>(new QueryData(projectionFields, projectionAliases) { IsProjectInto = true });
            if (isDistinct)
                q.Distinct();

            return aliasToField;
        }

        private static bool IsSelectStar(IReadOnlyList<Node> targetList)
        {
            return
                targetList.Count == 1 &&
                targetList[0].ResTarget?.Val?.ColumnRef?.Fields is { Count: 1 } fields &&
                fields[0].AStar != null;
        }

        // Distinguishes `SELECT a, b FROM t` (explicit, count-it) from `SELECT * FROM t` (wildcard,
        // don't count it). The empty-list case is treated as wildcard so we don't accidentally
        // suppress auto-include for queries the translator hasn't fully recognized yet.
        private static bool HasExplicitProjection(SelectStmt selectStmt)
        {
            var targets = selectStmt?.TargetList;
            if (targets == null || targets.Count == 0)
                return false;
            return IsSelectStar(targets) == false;
        }

        private static bool IsAggregateTarget(Node target)
        {
            return target.ResTarget?.Val?.FuncCall != null;
        }

        // Returns parallel arrays: projectionFields are the RQL expressions, projectionAliases
        // are the SQL `AS <alias>` names (defaulting to the field expression itself when the
        // SQL projection had no explicit alias). QueryData.Fields/Projections renders
        // `<field> as <alias>` when the two differ — which is the only way to preserve
        // PowerBI's aliases for constant projections like `1 as "c0"` (their row-preview
        // queries emit those and would otherwise produce a column-count mismatch).
        private static (string[] Fields, string[] Aliases, Dictionary<string, string> AliasToField) BuildColumnProjections(IReadOnlyList<Node> targetList, string fromAlias)
        {
            var projectionFields = new List<string>(capacity: targetList.Count);
            var projectionAliases = new List<string>(capacity: targetList.Count);
            var aliasToField = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var t in targetList)
            {
                var resTarget = t.ResTarget;
                var val = resTarget?.Val;
                if (val == null)
                    throw UnsupportedSelectProjection();

                var fieldName = TranslateSelectTargetValue(val, fromAlias);
                if (fieldName == null)
                    continue; // e.g. PowerBI pseudo-column json()

                projectionFields.Add(fieldName);

                // Use the SQL `AS <alias>` when explicit; otherwise the field expression itself
                // doubles as the alias (matches existing single-arg SelectFields semantics where
                // Fields[i] == Projections[i] and FieldsToFetchToken skips the `as` clause).
                var alias = resTarget.Name;
                if (string.IsNullOrWhiteSpace(alias))
                {
                    projectionAliases.Add(fieldName);
                    continue;
                }

                // SelectFields splices the `as <alias>` name verbatim; quote when needed. PowerBI
                // aliases columns to the synthetic id()/json() names, which are known tokens.
                projectionAliases.Add(EscapeProjectionField(alias));

                string sortableField = null;
                if (val.ColumnRef != null)
                {
                    var raw = ExtractFieldName(val, fromAlias);
                    if (PgSyntheticColumns.IsDocumentIdColumn(raw) == false)
                        sortableField = raw;
                }

                // A null marks an alias over a constant or id(): there is no document field to sort on.
                aliasToField[alias] = sortableField;
            }

            return (projectionFields.ToArray(), projectionAliases.ToArray(), aliasToField);
        }

        private static string TranslateSelectTargetValue(Node val, string fromAlias)
        {
            if (val.ColumnRef != null)
            {
                var fieldName = ExtractFieldName(val, fromAlias);
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new NotSupportedException("Unsupported column reference in SELECT projection");

                // Synthetic columns: `json` (or legacy `json()`) is metadata-blob, emitted by
                // the RQL row writer separately so we drop it from the projection. `id` (or
                // legacy `id()`) maps to RQL's id() function call regardless of which surface
                // name the client used.
                if (PgSyntheticColumns.IsJsonColumn(fieldName))
                    return null;

                if (PgSyntheticColumns.IsDocumentIdColumn(fieldName))
                    return "id()";

                return EscapeProjectionField(fieldName);
            }

            // PowerBI's row-preview queries decorate their projection list with constant
            // markers (e.g. `1 as "c0"`) so the client can count back a known fixed shape.
            // Without literal handling here the translator would throw NotSupported and the
            // whole query would fall through to a code path that strips the constant
            // (losing the column entirely), and PowerBI then reports
            // `Field count mismatch when mapping column types. 12 vs 11`.
            if (val.AConst != null)
            {
                if (TryRenderRqlLiteral(val.AConst, out var literal))
                    return literal;
            }

            throw UnsupportedSelectProjection();
        }

        // Renders the three concrete AConst kinds — Ival (integer), Fval (float), Sval
        // (string) — plus the all-null case (SQL NULL literal). Returns false for shapes the
        // RQL select clause can't accept verbatim, letting the caller fall through to the
        // unsupported-projection error. Strings are rendered via the same QuoteString helper the
        // WHERE translator uses (inner single quotes and backslashes escaped per RQL's convention).
        private static bool TryRenderRqlLiteral(A_Const c, out string rendered)
        {
            rendered = null;
            if (c == null)
                return false;

            if (c.Ival != null)
            {
                rendered = c.Ival.Ival.ToString(CultureInfo.InvariantCulture);
                return true;
            }

            if (c.Fval != null && string.IsNullOrEmpty(c.Fval.Fval) == false)
            {
                rendered = c.Fval.Fval;
                return true;
            }

            if (c.Sval != null && c.Sval.Sval != null)
            {
                rendered = QuoteString(c.Sval.Sval);
                return true;
            }

            if (c.Boolval != null)
            {
                rendered = c.Boolval.Boolval ? "true" : "false";
                return true;
            }

            // All null components → SQL NULL.
            if (c.Ival == null && c.Fval == null && c.Sval == null && c.Boolval == null)
            {
                rendered = "null";
                return true;
            }

            return false;
        }

        private static string GetFuncNameOrThrow(FuncCall funcCall)
        {
            if (funcCall.Funcname == null || funcCall.Funcname.Count == 0 || funcCall.Funcname[0].String == null)
                throw UnsupportedSelectAggregate();

            return (funcCall.Funcname[0].String.Sval ?? string.Empty).ToLowerInvariant();
        }

        private static Node GetSingleArgOrThrow(FuncCall funcCall)
        {
            if (funcCall.Args == null || funcCall.Args.Count != 1)
                throw UnsupportedSelectAggregate();

            return funcCall.Args[0];
        }

        private static void RejectUnsupportedAggregateModifiers(FuncCall funcCall)
        {
            if (funcCall.AggDistinct)
                throw UnsupportedAggregateModifier("DISTINCT");

            if (funcCall.AggFilter != null)
                throw UnsupportedAggregateModifier("FILTER (WHERE ...)");

            if (funcCall.Over != null)
                throw UnsupportedAggregateModifier("OVER (window function)");
        }

        private static string BuildCountProjection(FuncCall funcCall, string fromAlias = null)
        {
            RejectUnsupportedAggregateModifiers(funcCall);

            if (funcCall.AggStar)
                return "count()";

            var countArg = GetSingleArgOrThrow(funcCall);
            if (countArg.ColumnRef != null)
                throw UnsupportedCountArgument();

            // count(1) / count('x') count every row, which is exactly count(*).
            if (countArg.AConst != null)
                return "count()";

            throw UnsupportedSelectAggregate();
        }

        private static string BuildSingleColumnAggregateProjection(string funcName, FuncCall funcCall, string fromAlias = null)
        {
            RejectUnsupportedAggregateModifiers(funcCall);

            var arg0 = GetSingleArgOrThrow(funcCall);
            if (arg0?.ColumnRef == null)
                throw UnsupportedSelectAggregate();

            var fieldName = ExtractFieldName(arg0, fromAlias);
            if (string.IsNullOrWhiteSpace(fieldName))
                throw UnsupportedSelectAggregate();

            if (IsSafeRqlFieldPath(fieldName) == false)
                throw UnsupportedSelectAggregate();

            return $"{funcName}({fieldName})";
        }

        // An alias equal to the key adds nothing and would make RQL reject the duplicate alias.
        private static string BuildDistinctRowProjection(string field, string sqlAlias)
        {
            var projection = EscapeProjectionField(field);
            if (string.IsNullOrWhiteSpace(sqlAlias) ||
                string.Equals(sqlAlias, field, StringComparison.OrdinalIgnoreCase))
                return projection;

            return $"{projection} as {EscapeProjectionField(sqlAlias)}";
        }

        private static string BuildProjectionForGroupByTarget(Node val, string groupFieldName, string sqlAlias, string fromAlias = null)
        {
            if (val.ColumnRef == null)
                throw UnsupportedGroupBy();

            var field = ExtractFieldName(val, fromAlias);
            if (string.Equals(field, groupFieldName, StringComparison.OrdinalIgnoreCase) == false)
                throw UnsupportedGroupBy();

            if (IsSafeRqlFieldPath(groupFieldName) == false)
                throw UnsupportedGroupBy();

            if (string.IsNullOrWhiteSpace(sqlAlias) ||
                string.Equals(sqlAlias, groupFieldName, StringComparison.OrdinalIgnoreCase))
                return groupFieldName;

            if (IsSafeRqlIdentifier(sqlAlias) == false)
                throw UnsupportedGroupBy();

            return $"{groupFieldName} as {sqlAlias}";
        }

        // OrderField/Ordering describe how RQL refers to the aggregate in an `order by` clause:
        // count() reduces into the well-known 'Count' field, sum(x) into 'x' (see QueryMetadata's
        // GetSelectField). OutputName is the column name PostgreSQL would give the aggregate,
        // which is what an ORDER BY over the SELECT alias refers to.
        private readonly record struct GroupByAggregate(
            string Projection,
            string Expression,
            string OutputName,
            string OrderField,
            OrderingType Ordering);

        private static GroupByAggregate BuildGroupByAggregate(FuncCall funcCall, string sqlAlias, string fromAlias)
        {
            var funcName = GetFuncNameOrThrow(funcCall);

            string expr;
            string orderField;
            OrderingType ordering;

            switch (funcName)
            {
                case "count":
                    expr = BuildCountProjection(funcCall, fromAlias);
                    orderField = Constants.Documents.Indexing.Fields.CountFieldName;
                    ordering = OrderingType.Long;
                    break;

                case "sum":
                    expr = BuildSingleColumnAggregateProjection(funcName, funcCall, fromAlias);
                    orderField = ExtractFieldName(GetSingleArgOrThrow(funcCall), fromAlias);
                    ordering = OrderingType.Double;
                    break;

                // avg() is intentionally absent: RQL's grouped SELECT supports only count/sum,
                // so avg falls through to UnhandledQueryDiagnoser for a friendly message.
                default:
                    throw UnsupportedGroupBy();
            }

            var projection = expr;

            // Aggregates over a group-by key collide on RQL's implicit alias rule:
            // `sum(Freight)`'s implicit alias is `Freight`, identical to the group-by
            // key's own alias, and RQL rejects the SELECT with
            // `Duplicate alias 'Freight' detected`. Preserve the SQL's explicit AS clause
            // when present — PowerBI always emits one (`as "a0"`/`as "a1"`/…) — so the
            // RQL becomes `select Freight, sum(Freight) as a0` and the implicit alias
            // never matters.
            // Superset labels adhoc metrics COUNT(*) / SUM(col) / COUNT_DISTINCT(col) and uses the label
            // verbatim as the alias, so quote it rather than reject it. An alias that merely matches the
            // expression text still has to be emitted: RQL's implicit name for `sum(Freight)` is
            // `Freight`, so dropping `as "SUM(Freight)"` would rename the column out from under the client.
            if (string.IsNullOrWhiteSpace(sqlAlias) == false)
                projection = $"{expr} as {EscapeProjectionField(sqlAlias)}";

            return new GroupByAggregate(projection, expr, string.IsNullOrWhiteSpace(sqlAlias) ? funcName : sqlAlias, orderField, ordering);
        }

        private static string BuildAggregateProjectionForGroupByOrderBy(FuncCall funcCall, string fromAlias = null)
        {
            var funcName = GetFuncNameOrThrow(funcCall);
            return funcName switch
            {
                "count" => BuildCountProjection(funcCall, fromAlias),
                "sum" => BuildSingleColumnAggregateProjection(funcName, funcCall, fromAlias),
                _ => throw UnsupportedOrderByForGroupBy()
            };
        }

        private static void TranslateWhereClause(AsyncDocumentQuery<JObject> q, Node whereNode, string fromAlias)
        {
            if (SqlWhereParser.TryParse(whereNode, fromAlias, out var parsed) == false)
                throw new NotSupportedException("Unsupported WHERE clause");

            EmitWhere(q, parsed, wrapInSubclause: false);
        }

        private static void EmitWhere(AsyncDocumentQuery<JObject> q, ParsedWhere parsed, bool wrapInSubclause)
        {
            if (wrapInSubclause)
                q.OpenSubclause();

            switch (parsed)
            {
                case ParsedAnd a:
                    for (int i = 0; i < a.Children.Count; i++)
                    {
                        if (i > 0)
                            q.AndAlso();
                        // A nested OR under an AND needs parenthesisation to keep precedence.
                        EmitWhere(q, a.Children[i], wrapInSubclause: a.Children[i] is ParsedOr);
                    }
                    break;

                case ParsedOr o:
                    for (int i = 0; i < o.Children.Count; i++)
                    {
                        if (i > 0)
                            q.OrElse();
                        // AND has higher precedence than OR in RQL, so OR's children don't need wrapping.
                        EmitWhere(q, o.Children[i], wrapInSubclause: false);
                    }
                    break;

                case ParsedNot n:
                {
                    // LIKE carries its own negation flag (it emits a NULL guard around the negated form),
                    // so flip that rather than letting NegateNext() consume the guard instead.
                    if (n.Child is ParsedLike likeChild)
                    {
                        EmitWhere(q, likeChild with { Negated = likeChild.Negated == false }, wrapInSubclause: false);
                        break;
                    }

                    // RQL has no `NOT (expr)` syntax; instead the client side exposes NegateNext()
                    // which flips the polarity of the very next predicate. That maps cleanly when the
                    // child is a single primitive predicate (Binary / In / IsNull / Between). For a
                    // compound child (AND / OR / nested NOT), NegateNext() would only flip the first
                    // emitted predicate and silently drop the negation of the rest — better to fail
                    // explicitly than to silently return wrong rows.
                    if (n.Child is ParsedBinary or ParsedIn or ParsedIsNull or ParsedBetween)
                    {
                        q.NegateNext();
                        EmitWhere(q, n.Child, wrapInSubclause: false);
                        break;
                    }
                    throw new NotSupportedException("NOT over compound expressions is not supported in SQL to RQL WHERE translation");
                }

                case ParsedBinary b:
                {
                    var field = JoinPath(b.FieldPath);
                    var value = ToQueryValue(b.Value);
                    switch (b.Operator)
                    {
                        case "=":   q.WhereEquals(field, value); break;
                        case "!=":
                        case "<>":  q.WhereNotEquals(field, value); break;
                        case "<":   q.WhereLessThan(field, value); break;
                        case "<=":  q.WhereLessThanOrEqual(field, value); break;
                        case ">":   q.WhereGreaterThan(field, value); break;
                        case ">=":  q.WhereGreaterThanOrEqual(field, value); break;
                        default:    throw new NotSupportedException($"Unsupported operator '{b.Operator}'");
                    }
                    break;
                }

                case ParsedLike l:
                {
                    var field = JoinPath(l.FieldPath);
                    if (l.Pattern.Kind != ParsedValueKind.String || l.Pattern.Raw is not string pattern)
                        throw new NotSupportedException(SqlLikePattern.NonLiteralPattern);

                    if (SqlLikePattern.TryClassify(pattern, out var shape, out var literal, out var limitation) == false)
                        throw new NotSupportedException(limitation);

                    // LIKE is case-sensitive, so it needs exact() — an auto index stores the plain field
                    // through the lower-casing default analyzer, and only the exact() companion field keeps
                    // the original casing. ILIKE is the analyzed field's own (case-insensitive) behaviour.
                    var exact = l.CaseInsensitive == false;

                    // `x NOT LIKE p` is NULL - not true - when x is NULL, so PG drops the row. RQL's negation
                    // is "everything the positive match didn't hit", which keeps it; guard explicitly.
                    if (l.Negated)
                    {
                        q.OpenSubclause();
                        q.WhereNotEquals(field, null);
                        q.AndAlso();
                        q.NegateNext();
                    }

                    switch (shape)
                    {
                        case SqlLikeShape.Equals:     q.WhereEquals(field, literal, exact); break;
                        case SqlLikeShape.StartsWith: q.WhereStartsWith(field, literal, exact); break;
                        case SqlLikeShape.EndsWith:   q.WhereEndsWith(field, literal, exact); break;
                        default: throw new NotSupportedException(SqlLikePattern.NonLiteralPattern);
                    }

                    if (l.Negated)
                        q.CloseSubclause();
                    break;
                }

                case ParsedIn i:
                {
                    var field = JoinPath(i.FieldPath);
                    var values = new List<object>(i.Values.Count);
                    foreach (var v in i.Values)
                    {
                        if (v.Kind != ParsedValueKind.Parameter && v.Raw == null)
                            throw new NotSupportedException("Unsupported IN (null value)");
                        values.Add(ToQueryValue(v));
                    }
                    if (i.Negated)
                        q.NegateNext();
                    q.WhereIn(field, values);
                    break;
                }

                case ParsedBetween bt:
                {
                    var field = JoinPath(bt.FieldPath);
                    if ((bt.Lower.Kind != ParsedValueKind.Parameter && bt.Lower.Raw == null) ||
                        (bt.Upper.Kind != ParsedValueKind.Parameter && bt.Upper.Raw == null))
                        throw new NotSupportedException("Unsupported BETWEEN (missing bound)");
                    q.WhereBetween(field, ToQueryValue(bt.Lower), ToQueryValue(bt.Upper));
                    break;
                }

                case ParsedIsNull n:
                {
                    var field = JoinPath(n.FieldPath);
                    if (n.Negated)
                        q.WhereNotEquals(field, null);
                    else
                        q.WhereEquals(field, null);
                    break;
                }

                default:
                    throw new NotSupportedException($"Unsupported WHERE IR node: {parsed?.GetType().Name}");
            }

            if (wrapInSubclause)
                q.CloseSubclause();
        }

        private static string JoinPath(IReadOnlyList<string> fieldPath)
        {
            var joined = string.Join('.', fieldPath);

            // Raw name: WHERE predicates go through the query builder, which quotes the field. Only
            // ' and \ can break out of that quoting, so reject those.
            RejectQuoteBreakout(joined);

            return joined;
        }

        // Untrusted SQL identifiers reach RQL via the query builder (which quotes them) or a verbatim
        // SELECT field (quoted here). Both wrap spaces and punctuation but do NOT escape ' or \, which
        // would break out of RQL's single-quoted string - so reject those two characters.
        private static void RejectQuoteBreakout(string name)
        {
            if (name.Contains('\'') || name.Contains('\\'))
                throw new NotSupportedException("Unsupported identifier");
        }

        // SelectFields splices projection fields verbatim, so quote when needed; synthetic id()/json() stay raw.
        private static string EscapeProjectionField(string name)
        {
            if (PgSyntheticColumns.IsSyntheticColumn(name))
                return name;
            RejectQuoteBreakout(name);
            return QueryFieldUtil.EscapeIfNecessary(name, isPath: true);
        }

        // Literals pass through as-is; a $N placeholder becomes a PgBoundParameterReference marker
        // (see that record for how it survives to become an RQL parameter reference).
        private static object ToQueryValue(ParsedValue value)
            => value.Kind == ParsedValueKind.Parameter
                ? new PgBoundParameterReference((int)value.Raw)
                : value.Raw;

        private static void TranslateOrderBy(AsyncDocumentQuery<JObject> q, Google.Protobuf.Collections.RepeatedField<Node> sortClause, string fromAlias, IReadOnlyDictionary<string, OrderingType> sortTypeMap = null, IReadOnlyDictionary<string, string> aliasToField = null)
        {
            // Fail the translation rather than silently dropping a sort key we can't map to a column -
            // a missing ORDER BY term returns mis-ordered rows with no error (caller falls through to the diagnoser).
            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode.SortBy
                             ?? throw new NotSupportedException("Unsupported ORDER BY clause (only column sort keys are supported)");

                RejectUnsupportedSortModifiers(sortNode);

                var fieldName = ExtractFieldName(sortBy.Node, fromAlias);
                if (string.IsNullOrEmpty(fieldName))
                    throw new NotSupportedException("Unsupported ORDER BY expression (only column sort keys are supported)");

                if (TryResolveOrderByField(fieldName, aliasToField, out var resolved) == false)
                    throw new NotSupportedException($"Unsupported ORDER BY target \"{fieldName}\" (the SELECT alias does not name a document field)");

                fieldName = resolved;

                // Pick the right ordering: type-inferred from the sampled doc when we have it,
                // otherwise PG-style default (String / alphabetic).
                var ordering = OrderingType.String;
                if (sortTypeMap != null && sortTypeMap.TryGetValue(fieldName, out var inferred))
                    ordering = inferred;

                if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                    q.OrderByDescending(fieldName, ordering);
                else
                    q.OrderBy(fieldName, ordering);
            }
        }

        // RQL has no NULLS FIRST / NULLS LAST and no operator-driven sort, and its own null ordering is
        // not guaranteed to match what was asked for, so an approximation would sort by something else.
        private static void RejectUnsupportedSortModifiers(Node sortNode)
        {
            var sortBy = sortNode.SortBy;
            if (sortBy == null)
                return;

            if (sortBy.SortbyNulls is SortByNulls.First or SortByNulls.Last)
                throw new NotSupportedException("Unsupported ORDER BY clause (NULLS FIRST / NULLS LAST is not supported)");

            if (sortBy.SortbyDir == SortByDir.SortbyUsing)
                throw new NotSupportedException("Unsupported ORDER BY clause (ORDER BY ... USING is not supported)");
        }

        // ORDER BY may name a SELECT alias instead of a field. RQL sorts on the underlying document
        // field, so the alias has to be resolved back or the sort targets a field that does not exist.
        // False means the alias names a constant or id(), which has no field to sort on.
        private static bool TryResolveOrderByField(string fieldName, IReadOnlyDictionary<string, string> aliasToField, out string resolved)
        {
            if (aliasToField == null || aliasToField.TryGetValue(fieldName, out var aliased) == false)
            {
                resolved = fieldName;
                return true;
            }

            resolved = aliased;
            return aliased != null;
        }

        // Samples the first document of the collection to learn the blittable token type of each
        // ORDER BY field, mapped to RQL OrderingType. Returns null if the database isn't available
        // or the collection is empty — callers fall back to OrderingType.String in that case.
        //
        // Field lookups are case-sensitive against the document's stored property names. This
        // means an unquoted `ORDER BY Freight` (case-folded to `freight` by libpg_query) won't
        // find `Freight` and will fall back to String ordering — the same surface area issue users
        // hit with unquoted WHERE clauses. Quoting (`ORDER BY "Freight"`) preserves case and the
        // type inference kicks in.
        private static Dictionary<string, OrderingType> TryBuildSortTypeMap(
            DocumentDatabase documentDatabase,
            string collection,
            Google.Protobuf.Collections.RepeatedField<Node> sortClause,
            string fromAlias,
            IReadOnlyDictionary<string, string> aliasToField = null)
        {
            if (documentDatabase == null || string.IsNullOrWhiteSpace(collection))
                return null;

            // Collect candidate field names from the SortClause so we only do property lookups
            // for what we need.
            var fieldNames = new HashSet<string>(StringComparer.Ordinal);
            foreach (var sortNode in sortClause)
            {
                if (sortNode.SortBy?.Node == null)
                    continue;
                var name = ExtractFieldName(sortNode.SortBy.Node, fromAlias);
                if (string.IsNullOrEmpty(name))
                    continue;
                if (TryResolveOrderByField(name, aliasToField, out var resolved) == false)
                    continue;
                fieldNames.Add(resolved);
            }

            if (fieldNames.Count == 0)
                return null;

            BlittableJsonReaderObject sample = null;
            try
            {
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var doc in documentDatabase.DocumentsStorage.GetDocumentsFrom(context, collection, etag: 0, start: 0, take: 1))
                    {
                        sample = doc.Data;
                        break;
                    }

                    if (sample == null)
                        return null;

                    var result = new Dictionary<string, OrderingType>(StringComparer.Ordinal);
                    var prop = default(BlittableJsonReaderObject.PropertyDetails);
                    foreach (var name in fieldNames)
                    {
                        var propIdx = sample.GetPropertyIndex(name);
                        if (propIdx == -1)
                            continue;
                        sample.GetPropertyByIndex(propIdx, ref prop);
                        result[name] = MapTokenToOrderingType(prop.Token);
                    }
                    return result;
                }
            }
            catch
            {
                // Best-effort. Any failure (storage error, collection vanished, etc.) just falls
                // back to default String ordering — never block translation on the type probe.
                return null;
            }
        }

        private static OrderingType MapTokenToOrderingType(BlittableJsonToken token)
            => (token & BlittableJsonReaderBase.TypesMask) switch
            {
                BlittableJsonToken.Integer    => OrderingType.Long,
                BlittableJsonToken.LazyNumber => OrderingType.Double,
                _                              => OrderingType.String,
            };

        private static int? TranslateLimit(Node limitNode)
        {
            return PgSqlAstHelpers.TryReadNonNegativeIntConst(limitNode, out var value) ? value : null;
        }

        // Unquoted identifiers fold to lowercase (libpg_query); quote in SQL to preserve case.
        private static string ExtractFieldName(Node node, string fromAlias = null)
        {
            if (node?.ColumnRef != null && node.ColumnRef.Fields != null)
            {
                var fields = node.ColumnRef.Fields;
                if (fields.Count > 0)
                {
                    var parts = new string[fields.Count];
                    for (int i = 0; i < fields.Count; i++)
                    {
                        var s = GetStringValue(fields[i]);
                        if (s == null)
                            throw new NotSupportedException("Unsupported field reference");
                        parts[i] = s;
                    }

                    var joined = StripAliasPrefix(string.Join('.', parts), fromAlias);

                    // Raw name: the query builder quotes Where/OrderBy/GroupBy fields, SELECT emitters
                    // quote explicitly, and callers compare names case-insensitively. Only ' and \ can
                    // break out of the quoting, so reject those.
                    RejectQuoteBreakout(joined);

                    return joined;
                }
            }

            return null;
        }

        // Every dot-separated segment must be a plain RQL identifier (nested paths like a.b.c are fine).
        private static bool IsSafeRqlFieldPath(string path)
        {
            foreach (var segment in path.Split('.'))
            {
                if (IsSafeRqlIdentifier(segment) == false)
                    return false;
            }

            return true;
        }

        private static string StripAliasPrefix(string field, string fromAlias)
        {
            if (string.IsNullOrWhiteSpace(field) || string.IsNullOrWhiteSpace(fromAlias))
                return field;

            var prefix = fromAlias + ".";
            if (field.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return field[prefix.Length..];

            return field;
        }

        private static string GetStringValue(Node node)
        {
            return node.String?.Sval;
        }
    }
}
