using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using PgSqlParser;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Logging;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // Handles PowerBI's DirectQuery mode - outer SQL wrapping inner RQL - by recognizing the
    // wrapper shape, classifying it (grouped aggregate or simple projection), and rewriting the
    // resolved Raven.Server.Documents.Queries.AST.Query in place. This class focuses on the
    // PgQuery lifecycle plus the AST rewriters; recognition and shape classification live in
    // PowerBIWrapperRecognizer / PowerBIShapeClassifier, and the RQL is rendered by the canonical
    // StringQueryVisitor via Query.ToString().
    public sealed class PowerBIDirectQuery : PowerBIRqlQuery
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<PowerBIDirectQuery>();

        public PowerBIDirectQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, Dictionary<string, ReplaceColumnValue> replaces = null, int? limit = null)
            : base(queryString, parametersDataTypes, documentDatabase, replaces, limit)
        {
        }

        protected override bool IncludeDocumentIdColumn => false;

        protected override bool IncludePowerBIJsonColumn => false;

        protected override DynamicJsonValue BeforeRow(BlittableJsonReaderObject jsonResult, short? jsonIndex)
        {
            return null;
        }

        // No AfterRow override: the base PowerBIRqlQuery.AfterRow chain already short-circuits here
        // (json-write gated off by IncludePowerBIJsonColumn, no const-projection cells), so an empty
        // override would save nothing and would risk swallowing const-projection cells if ever passed.

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            pgQuery = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            try
            {
                var sql = queryText;

                var inner = PowerBIInnerRqlExtractor.TryExtractAndResolve(sql);
                if (inner == null)
                    return false;

                var selectStmt = inner.SanitizedSelectStmt;
                if (selectStmt == null)
                    return false;

                if (PowerBIWrapperRecognizer.TryNormalize(selectStmt, out var wrapper) == false)
                    return false;

                // Apply WHEREs found at intermediate wrapper levels to the resolved inner query
                // BEFORE either rewriter runs. PowerBI's DirectQuery routinely plants user filters
                // inside nested wrappers (e.g. between the null-ordering CASE helpers and the
                // distinct-grouping level), not at the outermost SELECT.
                //
                // Both rewriters consume inner.ResolvedQuery.Where: the grouped-aggregate path
                // preserves it via ShallowCopy, and the simple-direct path AND-merges it with the
                // outer WHERE downstream. So we just need to populate it here once.
                //
                // IMPORTANT: we filter out WHEREs that reference aggregate-output aliases (e.g.
                // `where not "_"."a0" is null` where `a0` came from `sum(Freight) as a0`). PowerBI
                // emits those as post-grouping null-guards; RQL gets the same effect implicitly
                // from GROUP BY semantics. Trying to translate them targets `a0` against the inner
                // Orders query, which has no such field, and RavenDB rejects the resulting RQL
                // ("Field 'a0' is neither an aggregation operation nor part of the group by key").
                if (wrapper.IntermediateWheres is { Count: > 0 })
                {
                    var aggregateOutputNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (wrapper.Aggregates != null)
                    {
                        foreach (var agg in wrapper.Aggregates)
                        {
                            if (string.IsNullOrEmpty(agg.OutputColumn) == false)
                                aggregateOutputNames.Add(agg.OutputColumn);
                        }
                    }

                    var iq = inner.ResolvedQuery;
                    if (iq.From.Alias == null)
                        iq.From.Alias = "_doc";

                    foreach (var iw in wrapper.IntermediateWheres)
                    {
                        if (IsAggregateOutputNullGuard(iw.WhereClause, aggregateOutputNames))
                            continue; // structural null-guard on an aggregate output - drop it (PowerBI artifact, not a user filter)

                        if (WhereClauseReferencesAnyColumn(iw.WhereClause, aggregateOutputNames))
                            return false; // real measure filter on an aggregate output - can't express post-grouping; fall through

                        if (PowerBIOuterWhereTranslator.TryTranslateWhere(iw.WhereClause, iw.WrapperAlias, iq.From.Alias, out var whereExpression) == false)
                            return false;

                        iq.Where = iq.Where == null
                            ? whereExpression
                            : new BinaryExpression(iq.Where, whereExpression, OperatorType.And);
                    }
                }

                if (PowerBIShapeClassifier.TryBuildGroupedAggregateShape(wrapper, out var aggregateShape))
                {
                    // A grouped RQL WHERE is post-reduction (HAVING): it may reference only group-by keys.
                    // If the merged WHERE (inner query + wrapper filters) touches any other field, the
                    // shape can't be expressed - fall through to the diagnoser rather than emit RQL the
                    // engine rejects with a raw error.
                    if (inner.ResolvedQuery.Where != null)
                    {
                        var groupKeys = new HashSet<string>(aggregateShape.GroupByFields, StringComparer.OrdinalIgnoreCase);
                        var whereFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        CollectFieldNames(inner.ResolvedQuery.Where, whereFields, inner.ResolvedQuery.From.Alias?.Value);
                        foreach (var f in whereFields)
                        {
                            if (groupKeys.Contains(f) == false)
                                return false;
                        }
                    }

                    string rewritten;
                    try
                    {
                        rewritten = RewriteGroupedAggregateRql(inner.ResolvedQuery, aggregateShape);
                    }
                    catch (Exception e)
                    {
                        PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIDirectQuery)}: grouped-aggregate RQL rewrite failed.", e);
                        return false;
                    }

                    if (string.IsNullOrWhiteSpace(rewritten))
                        return false;

                    if (IsValidRqlSelect(rewritten) == false)
                        return false;

                    pgQuery = new PowerBIDirectQuery(rewritten, parametersDataTypes, documentDatabase, replaces: null, limit: null);
                    return true;
                }

                // Scalar aggregates (no GROUP BY) are not supported in Raven RQL.
                if (wrapper.GroupByColumns is not { Count: > 0 } &&
                    wrapper.Aggregates is { Count: > 0 })
                    return false;

                if (PowerBIShapeClassifier.TryBuildDirectQueryShape(wrapper, out var shape) == false)
                    return false;

                var q = inner.ResolvedQuery;

                // Object projections require a from-alias.
                if (q.From.Alias == null)
                    q.From.Alias = "_doc";

                if (selectStmt.WhereClause != null)
                {
                    if (PowerBIOuterWhereTranslator.TryTranslateWhere(selectStmt.WhereClause, outerAlias: "_", innerAlias: q.From.Alias, out var whereExpression) == false)
                        return false;

                    q.Where = q.Where == null
                        ? whereExpression
                        : new BinaryExpression(q.Where, whereExpression, OperatorType.And);
                }

                var rewrittenRql = RewriteSimpleDirectQueryRql(
                    q,
                    projectionCols: shape.ProjectionCols,
                    shape.Limit);
                if (rewrittenRql == null)
                    return false;

                if (IsValidRqlSelect(rewrittenRql) == false)
                    return false;

                pgQuery = new PowerBIDirectQuery(rewrittenRql, parametersDataTypes, documentDatabase, replaces: null, limit: null);
                return true;
            }
            catch (Exception e)
            {
                PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIDirectQuery)}.{nameof(TryParse)} rejected query.", e);
                pgQuery = null;
                return false;
            }
        }

        // Mutates a shallow copy of the resolved RQL AST and emits the final string via the
        // canonical StringQueryVisitor (Query.ToString()) - no hand-rolled RQL fragments.
        // Returns null when the shape can't be expressed in the AST.
        private static string RewriteGroupedAggregateRql(Documents.Queries.AST.Query q, GroupedAggregateShape shape)
        {
            if (q == null || shape == null)
                return null;
            if (q.From.From == null)
                return null;
            if (shape.Aggregates is not { Count: > 0 } aggregates)
                return null;
            if (shape.GroupByFields is not { Count: > 0 } groupByFields)
                return null;

            // Mutate a shallow copy so the inner query AST passed in isn't disturbed.
            var core = q.ShallowCopy();
            core.IsDistinct = false;
            core.Filter = null;
            core.FilterLimit = null;
            core.Load = null;
            core.Include = null;
            core.CachedOrderBy = null;
            core.Offset = null;
            core.SelectFunctionBody = default;

            // GROUP BY: one FieldExpression per key.
            core.GroupBy = new List<(QueryExpression Expression, StringSegment? Alias)>(groupByFields.Count);
            foreach (var f in groupByFields)
                core.GroupBy.Add((MakeFieldExpression(f), null));

            // SELECT: keys, then aggregates. `count()` is argless (Raven's grouped RQL is
            // row-count, never field-count); other aggregates take the field as a single arg.
            core.Select = new List<(QueryExpression Expression, StringSegment? Alias)>(groupByFields.Count + aggregates.Count);
            foreach (var f in groupByFields)
                core.Select.Add((MakeFieldExpression(f), null));
            foreach (var agg in aggregates)
            {
                var args = IsCountFunction(agg.FunctionName)
                    ? new List<QueryExpression>()
                    : new List<QueryExpression> { MakeFieldExpression(agg.FieldName) };
                core.Select.Add((new MethodExpression(agg.FunctionName, args), new StringSegment(agg.OutputColumn)));
            }

            if (TryBuildOrderByAst(shape, out var orderBy) == false)
                return null;
            core.OrderBy = orderBy;

            // Default LIMIT preserves PowerBI's 1,000,001 cap when the wrapper didn't supply one.
            core.Limit = new ValueExpression(shape.Limit.ToString(CultureInfo.InvariantCulture), ValueTokenType.Long);

            // q.Where stays as-is: the classifier already guarantees the outer WHERE is just
            // a `<agg-output> IS NOT NULL` post-filter, which RQL gets implicitly via GROUP BY.

            var rql = core.ToString();
            return string.IsNullOrWhiteSpace(rql) ? null : rql;
        }

        private static FieldExpression MakeFieldExpression(string name)
            => new(new List<StringSegment> { new(name) });

        // True only for a structural null-guard on aggregate-output aliases: `<alias> IS NOT NULL`,
        // `NOT (<alias> IS NULL)`, or an AND/OR of such guards across measures (a two-measure visual
        // emits `not a0 is null or not a1 is null`). PowerBI emits these as post-grouping guards that
        // RQL's GROUP BY gives implicitly, so dropping them is safe. Any OTHER alias-referencing clause
        // (e.g. `a0 > 100`, `coalesce(a0,0) > 0`, or a guard OR'd with a real predicate) is a real
        // measure filter and must NOT be dropped.
        internal static bool IsAggregateOutputNullGuard(Node node, HashSet<string> aggregateOutputNames)
        {
            if (node == null || aggregateOutputNames is not { Count: > 0 })
                return false;

            // AND/OR of guards - droppable only if every branch is itself a guard on an aggregate output.
            if (node.BoolExpr is { Boolop: BoolExprType.AndExpr or BoolExprType.OrExpr, Args: { Count: > 0 } } boolExpr)
            {
                foreach (var arg in boolExpr.Args)
                {
                    if (IsAggregateOutputNullGuard(arg, aggregateOutputNames) == false)
                        return false;
                }
                return true;
            }

            ColumnRef colRef;
            if (node.NullTest is { Nulltesttype: NullTestType.IsNotNull } nt)
            {
                colRef = nt.Arg?.ColumnRef;
            }
            else if (node.BoolExpr is { Boolop: BoolExprType.NotExpr, Args: { Count: 1 } } be
                     && be.Args[0]?.NullTest is { Nulltesttype: NullTestType.IsNull } innerNt)
            {
                colRef = innerNt.Arg?.ColumnRef;
            }
            else
            {
                return false;
            }

            if (colRef?.Fields is not { Count: > 0 } fields)
                return false;

            var last = fields[^1]?.String?.Sval;
            return last != null && aggregateOutputNames.Contains(last);
        }

        // True if the clause references any aggregate-output alias anywhere (inside FuncCall, CaseExpr,
        // CoalesceExpr, etc. - PowerBI wraps the alias in `coalesce(a0,0) > 0`, `CASE WHEN a0 ...`).
        // Callers use it to fall through on a real measure filter; pure null-guards on the alias are
        // detected by IsAggregateOutputNullGuard and dropped instead.
        internal static bool WhereClauseReferencesAnyColumn(Node node, HashSet<string> columnNames)
        {
            if (node == null || columnNames is not { Count: > 0 })
                return false;

            return Walk(node);

            bool Walk(Node n)
            {
                if (n == null)
                    return false;

                if (n.ColumnRef?.Fields is { Count: > 0 } fields)
                {
                    var last = fields[^1]?.String?.Sval;
                    if (last != null && columnNames.Contains(last))
                        return true;
                }

                if (n.BoolExpr?.Args != null)
                {
                    foreach (var arg in n.BoolExpr.Args)
                        if (Walk(arg)) return true;
                }

                if (n.AExpr != null)
                {
                    if (Walk(n.AExpr.Lexpr)) return true;
                    if (Walk(n.AExpr.Rexpr)) return true;
                }

                if (n.NullTest?.Arg != null && Walk(n.NullTest.Arg))
                    return true;

                if (n.TypeCast?.Arg != null && Walk(n.TypeCast.Arg))
                    return true;

                if (n.RelabelType?.Arg != null && Walk(n.RelabelType.Arg))
                    return true;

                // Function calls - `coalesce(a0, 0)`, `nullif(a0, 0)`, `length(a0)`, etc.
                if (n.FuncCall?.Args != null)
                {
                    foreach (var arg in n.FuncCall.Args)
                        if (Walk(arg)) return true;
                }

                // CASE WHEN cond THEN result [WHEN ...] [ELSE defresult] END. Each WHEN node has
                // its own Expr (the condition) and Result (the value); Defresult is the ELSE.
                // CaseExpr.Arg is the optional "switch" expression (`CASE x WHEN ... THEN ...`).
                if (n.CaseExpr != null)
                {
                    if (n.CaseExpr.Arg != null && Walk(n.CaseExpr.Arg)) return true;
                    if (n.CaseExpr.Args != null)
                    {
                        foreach (var whenNode in n.CaseExpr.Args)
                        {
                            var when = whenNode?.CaseWhen;
                            if (when == null)
                                continue;
                            if (Walk(when.Expr)) return true;
                            if (Walk(when.Result)) return true;
                        }
                    }
                    if (n.CaseExpr.Defresult != null && Walk(n.CaseExpr.Defresult)) return true;
                }

                // COALESCE: PG's parser emits this as a dedicated CoalesceExpr node, NOT a
                // FuncCall - `coalesce(a, b, c)` becomes CoalesceExpr with Args=[a, b, c]. The alias
                // must be caught here too, since PowerBI wraps null-guards as `coalesce(a0, 0) > 0`.
                if (n.CoalesceExpr?.Args != null)
                {
                    foreach (var arg in n.CoalesceExpr.Args)
                        if (Walk(arg)) return true;
                }

                // GREATEST / LEAST: same pattern as COALESCE - dedicated MinMaxExpr node, not a
                // FuncCall. PowerBI occasionally uses these for null-safe comparisons.
                if (n.MinMaxExpr?.Args != null)
                {
                    foreach (var arg in n.MinMaxExpr.Args)
                        if (Walk(arg)) return true;
                }

                // SubLink: the testexpr is in our scope (e.g. `<x> IN (SELECT ...)`'s `<x>`).
                // We intentionally do NOT walk the inner Subselect - its column references live
                // in their own scope and aren't aggregate-alias matches at this level.
                if (n.SubLink?.Testexpr != null && Walk(n.SubLink.Testexpr))
                    return true;

                // List items - e.g. the right side of `x IN (a, b, c)` where the list contains
                // the candidate expressions.
                if (n.List?.Items != null)
                {
                    foreach (var item in n.List.Items)
                        if (Walk(item)) return true;
                }

                return false;
            }
        }

        private static bool TryBuildOrderByAst(GroupedAggregateShape shape, out List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending, NullsOrderingType NullsOrdering)> orderBy)
        {
            orderBy = null;

            if (shape.OrderByCols == null || shape.OrderByDescFlags == null)
                return false;

            if (shape.OrderByCols.Count != shape.OrderByDescFlags.Count)
                return false;

            if (shape.OrderByCols.Count == 0)
                return true; // null OrderBy means "no ORDER BY" - visitor skips it.

            var list = new List<(QueryExpression, OrderByFieldType, bool, NullsOrderingType)>(capacity: shape.OrderByCols.Count);
            for (int i = 0; i < shape.OrderByCols.Count; i++)
            {
                var col = shape.OrderByCols[i];
                var ascending = shape.OrderByDescFlags[i] == false;

                // Aggregate-output alias? Use the alias as a field reference and tag with the
                // numeric sort type matching the aggregate's RQL output kind (Long for count,
                // Double for sum) so the visitor emits the `as long` / `as double` cast PowerBI expects.
                int aggIndex = -1;
                for (int a = 0; a < shape.Aggregates.Count; a++)
                {
                    if (string.Equals(col, shape.Aggregates[a].OutputColumn, StringComparison.OrdinalIgnoreCase))
                    {
                        aggIndex = a;
                        break;
                    }
                }
                if (aggIndex >= 0)
                {
                    var agg = shape.Aggregates[aggIndex];
                    var fieldType = IsCountFunction(agg.FunctionName) ? OrderByFieldType.Long : OrderByFieldType.Double;
                    list.Add((MakeFieldExpression(agg.OutputColumn), fieldType, ascending, NullsOrderingType.Implicit));
                    continue;
                }

                // Otherwise must be a group-key match (case-insensitive).
                var foundGroupKey = false;
                foreach (var gb in shape.GroupByFields)
                {
                    if (string.Equals(col, gb, StringComparison.OrdinalIgnoreCase))
                    {
                        list.Add((MakeFieldExpression(gb), OrderByFieldType.Implicit, ascending, NullsOrderingType.Implicit));
                        foundGroupKey = true;
                        break;
                    }
                }
                if (foundGroupKey == false)
                    return false;
            }

            orderBy = list;
            return true;
        }

        // IsValidRqlSelect kept here - it's an emitter-side validation, not recognition.
        private static bool IsValidRqlSelect(string rql)
        {
            try
            {
                QueryMetadata.ParseQuery(rql, QueryType.Select);
                return true;
            }
            catch
            {
                return false;
            }
        }

        // AST-based simple direct-query emission. RQL's `select { ... }` object projection is an
        // opaque string in the Query AST (SelectFunctionBody), so the rebuild path hand-builds
        // that body text - but the surrounding clauses (Limit, etc.) are set through the AST and
        // rendered by the canonical StringQueryVisitor. The rebuilt body is threaded through
        // SelectFunctionBody so the visitor emits everything in canonical order rather than us
        // concatenating an RQL prefix with a hand-written `\nselect { ... }\nlimit 0, N` tail.
        private static string RewriteSimpleDirectQueryRql(Documents.Queries.AST.Query q, IReadOnlyList<string> projectionCols, int limit)
        {
            if (q == null)
                return null;

            if (projectionCols == null || projectionCols.Count == 0)
                return null;

            // A synthetic id()/json() column can't be a GROUP BY key. Rebuild an explicit object
            // projection (id() -> id(alias), json() -> the document, plain -> alias.field), no grouping.
            if (projectionCols.Any(PgSyntheticColumns.IsSyntheticColumn))
            {
                var synthetic = q.ShallowCopy();
                synthetic.IsDistinct = false;
                synthetic.Filter = null;
                synthetic.FilterLimit = null;
                synthetic.GroupBy = null;
                synthetic.OrderBy = null;
                synthetic.CachedOrderBy = null;
                synthetic.Offset = null;
                synthetic.Select = null;

                if (synthetic.From.Alias == null)
                    synthetic.From.Alias = "_doc";
                var projAlias = synthetic.From.Alias.Value;

                var body = new System.Text.StringBuilder("{ ");
                for (var i = 0; i < projectionCols.Count; i++)
                {
                    var col = projectionCols[i];
                    if (string.IsNullOrWhiteSpace(col))
                        return null;
                    if (i > 0)
                        body.Append(", ");
                    if (PgSyntheticColumns.IsJsonColumn(col))
                        body.Append($"\"{col}\" : {projAlias}");
                    else if (PgSyntheticColumns.IsDocumentIdColumn(col))
                        body.Append($"\"{col}\" : id({projAlias})");
                    else
                    {
                        // The projection body is JavaScript; `alias.col` is member access that can't be
                        // RQL-quoted, so drop the shape unless the column is a bare identifier.
                        if (RqlIdentifier.IsSafe(col) == false)
                            return null;
                        body.Append($"\"{col}\" : {projAlias}.{col}");
                    }
                }
                body.Append(" }");

                synthetic.SelectFunctionBody = (body.ToString(), null, null);

                if (limit >= 0)
                    synthetic.Limit = new ValueExpression(limit.ToString(CultureInfo.InvariantCulture), ValueTokenType.Long);

                var syntheticRql = synthetic.ToString();
                return string.IsNullOrWhiteSpace(syntheticRql) ? null : syntheticRql;
            }

            // id()-filtered object-projection inner: can't be grouped, but the id() filter already pins a
            // precise set - emit the inner projection as-is (PowerBI dedupes).
            if (q.SelectFunctionBody.FunctionText != null && WhereReferencesDocumentId(q.Where))
            {
                var passthrough = q.ShallowCopy();
                if (limit >= 0)
                    passthrough.Limit = new ValueExpression(limit.ToString(CultureInfo.InvariantCulture), ValueTokenType.Long);

                var passthroughRql = passthrough.ToString();
                return string.IsNullOrWhiteSpace(passthroughRql) ? null : passthroughRql;
            }

            // Direct-query shape always carries a GROUP BY (TryBuildDirectQueryShape requires it).
            // PowerBI's outer wrapper wraps the inner query with `GROUP BY <projected cols>` to ask
            // for tuple-distinct values - the same shape we handle for flat SQL via the
            // PgSqlToRqlTranslator path. We mirror that here: emit `from Coll group by <cols>
            // select <cols>` so the result is one row per distinct tuple.
            //
            // A per-document `select { ... }` projection would drop the GROUP BY (that form is
            // per-document, not per-group), returning every Orders row instead of distinct values -
            // which crashes PowerBI's chart engine in SubstituteWithIndex when its chart-bucket index
            // finds multiple raw rows mapping to a single logical category.
            var core = q.ShallowCopy();
            core.IsDistinct = false;
            core.Filter = null;
            core.FilterLimit = null;
            core.OrderBy = null;
            core.CachedOrderBy = null;
            core.Offset = null;
            core.SelectFunctionBody = (null, null, null);

            if (core.From.Alias == null)
                core.From.Alias = "_doc";
            var aliasText = core.From.Alias?.Value;

            // RQL constraint: WHERE clauses on grouped queries can only reference fields that are
            // GROUP BY keys (DynamicQueryMapping.Create throws `Field 'X' is neither an aggregation
            // operation nor part of the group by key` otherwise). PowerBI happily generates
            // `WHERE Company = 'X' GROUP BY Freight` - pre-grouping filter semantics - which
            // violates that rule.
            //
            // Workaround: collect every field name referenced by WHERE and add it to GROUP BY
            // (without adding it to SELECT, so the RowDescription matches PowerBI's expectation).
            // For a typical chart filter like `Company = 'CompanyA'` this just produces grouping
            // tuples `(Freight, 'CompanyA')` - collapsed back to distinct Freight values when
            // projected. For multi-value filters (`OR`, `IN`, ranges) the projected Freight set
            // may contain duplicates across the matching Company values, which PowerBI's local
            // chart aggregation will dedupe anyway.
            var groupByCols = new List<string>(projectionCols);
            if (core.Where != null)
            {
                var whereFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                CollectFieldNames(core.Where, whereFields, aliasText);
                foreach (var f in whereFields)
                {
                    if (groupByCols.Contains(f, StringComparer.OrdinalIgnoreCase))
                        continue;
                    groupByCols.Add(f);
                }
            }

            var groupBy = new List<(QueryExpression Expression, StringSegment? Alias)>(groupByCols.Count);
            foreach (var col in groupByCols)
            {
                if (string.IsNullOrWhiteSpace(col))
                    return null;
                groupBy.Add((BuildFieldExpression(col), null));
            }

            var select = new List<(QueryExpression Expression, StringSegment? Alias)>(projectionCols.Count);
            foreach (var col in projectionCols)
            {
                if (string.IsNullOrWhiteSpace(col))
                    return null;
                select.Add((BuildFieldExpression(col), null));
            }

            core.GroupBy = groupBy;
            core.Select = select;

            if (limit >= 0)
                core.Limit = new ValueExpression(limit.ToString(CultureInfo.InvariantCulture), ValueTokenType.Long);

            var rql = core.ToString();
            return string.IsNullOrWhiteSpace(rql) ? null : rql;
        }

        private static FieldExpression BuildFieldExpression(string fieldName)
        {
            return new FieldExpression(new List<StringSegment> { new StringSegment(fieldName) });
        }

        // Walk a WHERE-style QueryExpression tree and collect every bare field name it references,
        // stripping the from-clause alias prefix when present. Used to amend GROUP BY for filtered
        // tuple-distinct queries (see comment in RewriteSimpleDirectQueryRql).
        private static void CollectFieldNames(QueryExpression expr, HashSet<string> fields, string aliasToStrip)
        {
            switch (expr)
            {
                case null:
                    return;
                case FieldExpression fe:
                    if (fe.Compound == null || fe.Compound.Count == 0)
                        return;
                    var startIdx = 0;
                    if (aliasToStrip != null && fe.Compound.Count > 1 &&
                        string.Equals(fe.Compound[0].Value, aliasToStrip, StringComparison.OrdinalIgnoreCase))
                        startIdx = 1;
                    if (startIdx < fe.Compound.Count)
                        fields.Add(fe.Compound[startIdx].Value);
                    return;
                case BinaryExpression be:
                    CollectFieldNames(be.Left, fields, aliasToStrip);
                    CollectFieldNames(be.Right, fields, aliasToStrip);
                    return;
                case NegatedExpression ne:
                    CollectFieldNames(ne.Expression, fields, aliasToStrip);
                    return;
                case BetweenExpression bet:
                    CollectFieldNames(bet.Source, fields, aliasToStrip);
                    return;
                case InExpression ie:
                    CollectFieldNames(ie.Source, fields, aliasToStrip);
                    return;
                case MethodExpression me:
                    if (me.Arguments != null)
                    {
                        foreach (var arg in me.Arguments)
                            CollectFieldNames(arg, fields, aliasToStrip);
                    }
                    return;
                // ValueExpression literals contribute no field references.
            }
        }

        // True if the RQL WHERE tree references the document-id method id(). Such a filter pins a
        // precise document set that can't be re-grouped (id() is neither a stored field nor a valid
        // group key) - see the passthrough branch in RewriteSimpleDirectQueryRql.
        private static bool WhereReferencesDocumentId(QueryExpression expr)
        {
            switch (expr)
            {
                case null:
                    return false;
                case MethodExpression me:
                    if (me.Arguments is not { Count: > 0 })
                        return string.Equals(me.Name.Value, "id", StringComparison.OrdinalIgnoreCase);
                    foreach (var arg in me.Arguments)
                        if (WhereReferencesDocumentId(arg))
                            return true;
                    return false;
                case BinaryExpression be:
                    return WhereReferencesDocumentId(be.Left) || WhereReferencesDocumentId(be.Right);
                case NegatedExpression ne:
                    return WhereReferencesDocumentId(ne.Expression);
                case BetweenExpression bet:
                    return WhereReferencesDocumentId(bet.Source);
                case InExpression ie:
                    return WhereReferencesDocumentId(ie.Source);
                default:
                    return false;
            }
        }

        // Raven grouped RQL uses count() with no argument.
        private static bool IsCountFunction(string name) =>
            string.Equals(name, "count", StringComparison.OrdinalIgnoreCase);

    }
}
