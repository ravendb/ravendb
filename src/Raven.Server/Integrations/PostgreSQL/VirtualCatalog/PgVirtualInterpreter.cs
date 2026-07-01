using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PgSqlParser;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    internal static class PgVirtualInterpreter
    {
        public static bool TryExecute(string queryText, VirtualQueryContext ctx, out PgTable result)
        {
            result = null;

            if (SelectStmtShape.TryParseSelectStatements(queryText, out var selects) == false)
                return false;

            if (selects.Count == 1)
                return TryExecuteSingle(selects[0], ctx, out result);

            // Multi-statement batch (e.g. Npgsql's `select version(); select current_setting('max_index_keys')`).
            // Each statement must produce a single-row result; merge the rows column-wise.
            var parts = new List<PgTable>(selects.Count);
            foreach (var s in selects)
            {
                if (TryExecuteSingle(s, ctx, out var sub) == false)
                    return false;
                if (sub.Data.Count != 1)
                    return false;
                parts.Add(sub);
            }

            result = MergeColumnWise(parts);
            return true;
        }

        private static bool TryExecuteSingle(SelectStmt selectStmt, VirtualQueryContext ctx, out PgTable result)
            => TryExecuteSingleWithOuterScope(selectStmt, ctx, outerScope: null, out result);

        // outerScope is non-null only when the SELECT is a correlated subquery - its inner column
        // lookups can then walk up to the enclosing row.
        private static bool TryExecuteSingleWithOuterScope(SelectStmt selectStmt, VirtualQueryContext ctx, RowScope outerScope, out PgTable result)
        {
            result = null;

            if (RejectUnsupportedClauses(selectStmt))
                return false;

            // WITH / WITH RECURSIVE: materialize each CTE into ctx.Ctes before running the body.
            // Save/restore the existing Ctes map so nested WITH clauses don't leak across queries.
            var previousCtes = ctx.Ctes;
            try
            {
                if (selectStmt.WithClause is { Ctes.Count: > 0 })
                {
                    if (TryMaterializeWithClause(selectStmt.WithClause, ctx, outerScope) == false)
                        return false;
                }

                // UNION / UNION ALL at the top level. Used by Microsoft Fabric Copy Job's
                // "Choose data" picker (`information_schema.tables UNION information_schema.views`)
                // and by pgAdmin variants. UNION ALL inside a CTE body for WITH RECURSIVE is
                // handled in TryMaterializeRecursiveCte; this branch covers the OUTERMOST set op.
                // Must run BEFORE HasNoFromClause - a top-level UNION's outer SelectStmt has no
                // FROM clause of its own (only Larg/Rarg), so the FROM-less path would otherwise
                // grab it and try to evaluate it as `select <expr>` with no targets.
                if (selectStmt.Op == SetOperation.SetopUnion)
                {
                    return TryExecuteUnion(selectStmt, ctx, outerScope, out result);
                }
                if (selectStmt.Op != SetOperation.SetopNone)
                {
                    // INTERSECT / EXCEPT are not implemented - return false so the next
                    // dispatcher arm gets a chance, and UnhandledQueryDiagnoser surfaces a
                    // targeted message instead of a generic SQL dump. Falling through to
                    // HasNoFromClause would be wrong: the outer SelectStmt has no FromClause
                    // and no TargetList; the targets live on the arms.
                    return false;
                }

                if (SelectStmtShape.HasNoFromClause(selectStmt))
                {
                    if (TryExecuteScalarFunction(selectStmt, ctx, out result))
                        return true;
                    // Fall through to the generic expression path: SELECT <expr> AS <alias> without
                    // FROM, where <expr> can be a CASE, a scalar subquery, a literal, etc.
                    return TryExecuteNoFromExpression(selectStmt, ctx, outerScope, out result);
                }

                return TryExecuteTableQuery(selectStmt, ctx, outerScope, out result);
            }
            finally
            {
                ctx.Ctes = previousCtes;
            }
        }

        // Executes a top-level UNION / UNION ALL. PG semantics:
        //   - Output column NAMES come from the LEFT arm.
        //   - Both arms must project the same number of columns.
        //   - UNION dedupes; UNION ALL keeps duplicates.
        //   - The outermost SelectStmt's SortClause / LimitCount / LimitOffset apply to the
        //     COMBINED result (not each arm individually), and the SortClause references
        //     output column names which match the left arm's projection.
        //
        // We run both arms via TryExecuteAsRows to stay on the raw object[] row representation
        // until the very end - that lets us reuse the existing sort/limit/dedupe machinery
        // before serializing to wire bytes via BuildPgTable.
        private static bool TryExecuteUnion(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out PgTable result)
        {
            result = null;

            if (s.Larg == null || s.Rarg == null)
                return false;

            if (TryExecuteSingleWithOuterScope(s.Larg, ctx, outerScope, out var leftTable) == false || leftTable == null)
                return false;
            if (TryExecuteSingleWithOuterScope(s.Rarg, ctx, outerScope, out var rightTable) == false || rightTable == null)
                return false;

            // PG requires same column count across arms; we honor that. Cross-arm type coercion
            // (e.g. promote int to numeric to match the right arm's wider type) isn't done - for
            // the empty-views + tables case the arms are structurally identical anyway.
            if (leftTable.Columns.Count != rightTable.Columns.Count)
                return false;

            var combined = new List<PgDataRow>(leftTable.Data.Count + rightTable.Data.Count);
            combined.AddRange(leftTable.Data);
            combined.AddRange(rightTable.Data);

            // UNION (not ALL) dedupes. We key off a string-encoded form of each row - adequate
            // for the catalog-list shapes that drive this code path (name/text columns, small
            // row counts). If/when this gets used for big numeric or binary payloads we'd want
            // a typed comparer instead.
            if (s.All == false)
                combined = DedupCombinedRows(combined);

            // Outer ORDER BY / LIMIT / OFFSET. Sort plan is built against the LEFT arm's
            // projected output (output column names come from the left).
            if (s.SortClause is { Count: > 0 })
            {
                if (TryBuildSetOpSortPlan(s.SortClause, leftTable.Columns, out var sortPlan) == false)
                    return false;
                if (sortPlan.Count > 0)
                    combined = SortPgRowsByBytes(combined, sortPlan);
            }

            int offset = 0;
            int? limit = null;
            if (s.LimitOffset != null)
            {
                if (PgSqlAstHelpers.TryReadNonNegativeIntConst(s.LimitOffset, out offset) == false)
                    return false;
            }
            if (s.LimitCount != null)
            {
                if (PgSqlAstHelpers.TryReadNonNegativeIntConst(s.LimitCount, out var l) == false)
                    return false;
                limit = l;
            }
            if (offset > 0)
                combined = combined.Skip(offset).ToList();
            if (limit.HasValue)
                combined = combined.Take(limit.Value).ToList();

            result = new PgTable
            {
                Columns = leftTable.Columns,
                Data = combined,
            };
            return true;
        }

        private static List<PgDataRow> DedupCombinedRows(List<PgDataRow> rows)
        {
            if (rows.Count == 0)
                return rows;
            var seen = new HashSet<string>(rows.Count);
            var output = new List<PgDataRow>(rows.Count);
            foreach (var row in rows)
            {
                if (seen.Add(BytesRowKey(row)))
                    output.Add(row);
            }
            return output;
        }

        private static string BytesRowKey(PgDataRow row)
        {
            // Per-column wire bytes, base64-encoded so we can use a string HashSet without
            // worrying about embedded NULs or separator collisions. Null cells get a sentinel.
            var sb = new System.Text.StringBuilder();
            var span = row.ColumnData.Span;
            for (int i = 0; i < span.Length; i++)
            {
                if (i > 0)
                    sb.Append('|');
                if (span[i] is null)
                    sb.Append("__NULL__");
                else
                    sb.Append(System.Convert.ToBase64String(span[i].Value.Span));
            }
            return sb.ToString();
        }

        private static bool TryBuildSetOpSortPlan(Google.Protobuf.Collections.RepeatedField<Node> sortClause, List<PgColumn> outputColumns, out List<SortKey> plan)
        {
            plan = new List<SortKey>();
            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode?.SortBy;
                if (sortBy?.Node?.ColumnRef?.Fields is not { Count: 1 } fields)
                    return false;
                var name = fields[0]?.String?.Sval;
                if (string.IsNullOrEmpty(name))
                    return false;
                int idx = -1;
                for (int i = 0; i < outputColumns.Count; i++)
                {
                    if (string.Equals(outputColumns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        idx = i;
                        break;
                    }
                }
                if (idx == -1)
                    return false;
                bool descending = sortBy.SortbyDir == SortByDir.SortbyDesc;
                plan.Add(new SortKey(idx, descending));
            }
            return true;
        }

        private static List<PgDataRow> SortPgRowsByBytes(List<PgDataRow> rows, List<SortKey> plan)
        {
            rows.Sort((a, b) =>
            {
                foreach (var key in plan)
                {
                    var av = a.ColumnData.Span[key.CellIndex];
                    var bv = b.ColumnData.Span[key.CellIndex];
                    int cmp;
                    if (av == null && bv == null) cmp = 0;
                    else if (av == null) cmp = -1;
                    else if (bv == null) cmp = 1;
                    else cmp = av.Value.Span.SequenceCompareTo(bv.Value.Span);
                    if (cmp != 0)
                        return key.Descending ? -cmp : cmp;
                }
                return 0;
            });
            return rows;
        }

        // Materializes each CTE in the WITH clause into ctx.Ctes. For non-recursive WITH the body
        // is evaluated once. For WITH RECURSIVE the body is `<base> UNION ALL <recursive>`: seed
        // with the base case, then iterate the recursive case until it produces no new rows.
        //
        // This isn't a textbook implementation - standard PG passes only the latest iteration's
        // "delta" rows to the recursive case, while we just expose the full accumulated CTE. Good
        // enough for pgAdmin's role-membership probe (recursive case joins through pg_auth_members,
        // which is empty, so we terminate immediately). The MaxIterations guard keeps us safe if a
        // future caller writes a recursive case that does converge through a different path.
        private const int MaxCteIterations = 256;

        private static bool TryMaterializeWithClause(WithClause withClause, VirtualQueryContext ctx, RowScope outerScope)
        {
            ctx.Ctes ??= new Dictionary<string, MaterializedCte>(StringComparer.OrdinalIgnoreCase);

            foreach (var cteNode in withClause.Ctes)
            {
                var cte = cteNode?.CommonTableExpr;
                if (cte == null)
                    return false;
                var name = cte.Ctename;
                var bodyStmt = cte.Ctequery?.SelectStmt;
                if (string.IsNullOrEmpty(name) || bodyStmt == null)
                    return false;

                if (withClause.Recursive
                    && bodyStmt.Op == SetOperation.SetopUnion
                    && bodyStmt.All
                    && bodyStmt.Larg != null
                    && bodyStmt.Rarg != null)
                {
                    if (TryMaterializeRecursiveCte(name, bodyStmt.Larg, bodyStmt.Rarg, ctx, outerScope) == false)
                        return false;
                }
                else
                {
                    // Non-recursive: a plain SELECT body.
                    if (TryExecuteSingleWithOuterScope(bodyStmt, ctx, outerScope, out var pg) == false)
                        return false;
                    ctx.Ctes[name] = MaterializedCteFromPgTable(name, pg);
                }
            }
            return true;
        }

        private static bool TryMaterializeRecursiveCte(string name, SelectStmt baseCase, SelectStmt recursiveCase, VirtualQueryContext ctx, RowScope outerScope)
        {
            if (TryExecuteSingleWithOuterScope(baseCase, ctx, outerScope, out var basePg) == false)
                return false;

            var materialized = MaterializedCteFromPgTable(name, basePg);
            ctx.Ctes[name] = materialized;

            for (int i = 0; i < MaxCteIterations; i++)
            {
                int rowsBefore = materialized.Rows.Count;
                if (TryExecuteSingleWithOuterScope(recursiveCase, ctx, outerScope, out var iter) == false)
                    return false;
                if (iter.Data.Count == 0)
                    return true; // fixed point
                AppendPgRowsToCte(materialized, iter);
                if (materialized.Rows.Count == rowsBefore)
                    return true; // no new rows added (e.g. all returned rows were duplicates of existing)
            }
            // Exhausted iteration cap without converging - refuse rather than loop forever.
            return false;
        }

        private static MaterializedCte MaterializedCteFromPgTable(string name, PgTable pg)
        {
            var virtualColumns = new List<PgVirtualColumn>(pg.Columns.Count);
            foreach (var c in pg.Columns)
                virtualColumns.Add(new PgVirtualColumn(c.Name, c.PgType, c.FormatCode));

            var rows = new List<object[]>(pg.Data.Count);
            foreach (var dataRow in pg.Data)
                rows.Add(DecodeRow(dataRow, pg.Columns));

            return new MaterializedCte { Name = name, Columns = virtualColumns, Rows = rows };
        }

        private static void AppendPgRowsToCte(MaterializedCte materialized, PgTable iter)
        {
            foreach (var dataRow in iter.Data)
                materialized.Rows.Add(DecodeRow(dataRow, iter.Columns));
        }

        private static object[] DecodeRow(PgDataRow dataRow, ICollection<PgColumn> columns)
        {
            var span = dataRow.ColumnData.Span;
            var row = new object[columns.Count];
            int i = 0;
            foreach (var col in columns)
            {
                var cell = span[i];
                row[i] = cell.HasValue ? col.PgType.FromBytes(cell.Value.ToArray(), PgFormat.Text) : null;
                i++;
            }
            return row;
        }

        // No-FROM expression fallback: evaluates a single-target SELECT against an empty row scope.
        // Used for pgAdmin-style probes like `SELECT CASE WHEN (SELECT ...) > 0 THEN 'x' ... END`.
        private static bool TryExecuteNoFromExpression(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out PgTable result)
        {
            result = null;
            if (s.TargetList is not { Count: 1 } targetList)
                return false;

            var rt = targetList[0]?.ResTarget;
            if (rt?.Val == null)
                return false;

            var scope = RowScope.Builder().Build();
            if (outerScope != null)
                scope = scope.WithParent(outerScope);
            if (ExpressionEvaluator.TryEvaluate(rt.Val, scope, MakeSubqueryResolver(ctx), MakeFunctionResolver(ctx), out var value) == false)
                return false;

            var pgType = InferPgTypeFromRuntimeValue(value);
            var outName = string.IsNullOrWhiteSpace(rt.Name) ? "?column?" : rt.Name;
            var bytes = value == null ? (ReadOnlyMemory<byte>?)null : pgType.ToBytes(value, PgFormat.Text);

            result = new PgTable
            {
                Columns = new List<PgColumn> { new(outName, columnIndex: 0, pgType: pgType, formatCode: PgFormat.Text) },
                Data = new List<PgDataRow> { new(new ReadOnlyMemory<byte>?[] { bytes }) },
            };
            return true;
        }

        // Pgsql subquery resolver. Executes the inner SELECT and returns ALL its single-column
        // row values; ExpressionEvaluator decides whether to treat them as a scalar (EXPR_SUBLINK)
        // or an array (ARRAY_SUBLINK). The outerScope is forwarded so the inner SELECT can
        // correlate (e.g. `WHERE inner.id = outer.id`).
        private static ExpressionEvaluator.ScalarSubqueryResolver MakeSubqueryResolver(VirtualQueryContext ctx)
        {
            return (SelectStmt subquery, RowScope outerScope, out IReadOnlyList<object> values) =>
            {
                values = null;
                if (subquery == null)
                    return false;
                if (TryExecuteSingleWithOuterScope(subquery, ctx, outerScope, out var sub) == false)
                    return false;
                if (sub.Columns.Count != 1)
                    return false;

                var list = new List<object>(sub.Data.Count);
                foreach (var row in sub.Data)
                {
                    var cell = row.ColumnData.Span[0];
                    if (cell.HasValue == false)
                    {
                        list.Add(null);
                        continue;
                    }
                    list.Add(sub.Columns[0].PgType.FromBytes(cell.Value.ToArray(), PgFormat.Text));
                }
                values = list;
                return true;
            };
        }

        // Resolves inline scalar function calls - `current_database()`, `pg_encoding_to_char(x)`,
        // etc. - by looking up the function in PgVirtualDatabase and threading the per-connection
        // VirtualQueryContext through (current_database needs ctx.Database.Name).
        private static ExpressionEvaluator.ScalarFunctionResolver MakeFunctionResolver(VirtualQueryContext ctx)
        {
            return (string name, IReadOnlyList<object> args, out object value) =>
            {
                value = null;
                if (PgVirtualDatabase.TryGetFunction(name, out var function) == false)
                    return false;
                return function.TryEvaluate(args, ctx, out value);
            };
        }

        // Loose PgType inference for the no-FROM expression path. Match the value's runtime type
        // - good enough for the scalar values we see (bool, long, string).
        private static PgType InferPgTypeFromRuntimeValue(object value)
        {
            return value switch
            {
                bool   => PgBool.Default,
                long   => PgInt8.Default,
                int    => PgInt4.Default,
                double => PgFloat8.Default,
                _      => PgText.Default,
            };
        }

        private static PgTable MergeColumnWise(IReadOnlyList<PgTable> parts)
        {
            var totalColumns = 0;
            foreach (var p in parts)
                totalColumns += p.Columns.Count;

            var columns = new List<PgColumn>(totalColumns);
            var cells = new ReadOnlyMemory<byte>?[totalColumns];

            short outIndex = 0;
            foreach (var part in parts)
            {
                var srcRow = part.Data[0].ColumnData.Span;
                for (int i = 0; i < part.Columns.Count; i++)
                {
                    var src = part.Columns[i];
                    columns.Add(new PgColumn(src.Name, outIndex, src.PgType, src.FormatCode));
                    cells[outIndex] = srcRow[i];
                    outIndex++;
                }
            }

            return new PgTable
            {
                Columns = columns,
                Data = new List<PgDataRow> { new(cells) }
            };
        }

        private static bool RejectUnsupportedClauses(SelectStmt s)
        {
            if (s.GroupClause is { Count: > 0 })
                return true;
            if (s.DistinctClause is { Count: > 0 })
                return true;
            return false;
        }

        // Scalar-function path: SELECT version(), SELECT current_setting('x'), etc.
        private static bool TryExecuteScalarFunction(SelectStmt s, VirtualQueryContext ctx, out PgTable result)
        {
            result = null;

            if (s.TargetList is not { Count: 1 } targetList)
                return false;

            var resTarget = targetList[0]?.ResTarget;
            var funcCall = resTarget?.Val?.FuncCall;
            if (funcCall == null)
                return false;

            if (funcCall.Funcname is not { Count: 1 })
                return false;

            var name = funcCall.Funcname[0].String?.Sval;
            if (string.IsNullOrWhiteSpace(name))
                return false;

            if (PgVirtualDatabase.TryGetFunction(name, out var function) == false)
                return false;

            var args = new List<object>();
            if (funcCall.Args != null)
            {
                foreach (var arg in funcCall.Args)
                {
                    if (TryReadConstantArg(arg, out var value) == false)
                        return false;
                    args.Add(value);
                }
            }

            if (function.TryEvaluate(args, ctx, out var output) == false)
                return false;

            var outputName = string.IsNullOrWhiteSpace(resTarget.Name) == false
                ? resTarget.Name
                : function.ResultColumnName;

            var columns = new List<PgColumn>
            {
                new(outputName, columnIndex: 0, pgType: function.PgType, formatCode: PgFormat.Text),
            };

            var rowData = new ReadOnlyMemory<byte>?[]
            {
                output == null ? (ReadOnlyMemory<byte>?)null : function.PgType.ToBytes(output, PgFormat.Text),
            };

            result = new PgTable
            {
                Columns = columns,
                Data = new List<PgDataRow> { new(rowData) },
            };
            return true;
        }

        private static bool TryReadConstantArg(Node node, out object value)
        {
            value = null;
            var c = node?.AConst;
            if (c == null)
                return false;

            if (c.Sval != null && c.Sval.Sval != null)
            {
                value = c.Sval.Sval;
                return true;
            }

            if (c.Ival != null)
            {
                value = (long)c.Ival.Ival;
                return true;
            }

            return false;
        }

        // FROM-bearing path: build a joined-row stream via JoinExecutor, evaluate WHERE on each row,
        // project each target through ExpressionEvaluator, optionally sort/limit, and serialize the
        // surviving rows into a PgTable.
        private static bool TryExecuteTableQuery(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out PgTable result)
        {
            result = null;
            // Aggregate-without-GROUP-BY path: `SELECT count(...) FROM t WHERE ...` collapses the
            // filtered input to a single result row. Used by pgAdmin's existence probes.
            if (TryExecuteAggregateWithoutGroupBy(s, ctx, outerScope, out result))
                return true;
            if (TryExecuteAsRows(s, ctx, outerScope, out var columns, out var rows) == false)
                return false;
            result = BuildPgTable(columns, rows);
            return true;
        }

        // Detects `SELECT <aggregate>(...) [, ...] FROM t [WHERE ...]` with no GROUP BY and every
        // target being an aggregate (currently only COUNT - extend when other aggregates show up).
        // Emits a single result row containing the aggregate values.
        private static bool TryExecuteAggregateWithoutGroupBy(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out PgTable result)
        {
            result = null;

            if (s.GroupClause is { Count: > 0 })
                return false;
            if (s.TargetList is not { Count: > 0 } targetList)
                return false;

            // Every target must be an aggregate FuncCall - mixing aggregates and bare columns
            // without GROUP BY is a SQL error and we don't try to recover from it.
            foreach (var t in targetList)
            {
                var rt = t?.ResTarget;
                if (rt?.Val?.FuncCall == null || IsAggregateFunctionCall(rt.Val.FuncCall) == false)
                    return false;
            }

            var executor = new JoinExecutor(ctx, (sub, alias) => TryResolveSubquery(sub, alias, ctx));
            if (executor.TryExecute(s.FromClause, out var joinedRows, out var sources) == false)
                return false;

            var subqueryResolver = MakeSubqueryResolver(ctx);
            var functionResolver = MakeFunctionResolver(ctx);

            if (TryApplyWhereFilter(s.WhereClause, joinedRows, sources, outerScope, subqueryResolver, functionResolver, out var filtered) == false)
                return false;

            var columns = new List<PgColumn>(targetList.Count);
            var cells = new ReadOnlyMemory<byte>?[targetList.Count];
            for (int i = 0; i < targetList.Count; i++)
            {
                var rt = targetList[i].ResTarget;
                var func = rt.Val.FuncCall;
                var aggName = func.Funcname[0].String?.Sval;
                if (TryComputeAggregate(aggName, func, filtered, sources, outerScope, subqueryResolver, functionResolver, out var value) == false)
                    return false;

                var outName = string.IsNullOrWhiteSpace(rt.Name) ? aggName : rt.Name;
                columns.Add(new PgColumn(outName, columnIndex: (short)i, pgType: PgInt8.Default, formatCode: PgFormat.Text));
                cells[i] = value == null ? null : PgInt8.Default.ToBytes(value, PgFormat.Text);
            }

            result = new PgTable
            {
                Columns = columns,
                Data = new List<PgDataRow> { new(cells) },
            };
            return true;
        }

        // Filters joined rows by an optional WHERE clause, evaluating each row against an
        // ExpressionEvaluator scope chained onto outerScope (for correlated subqueries). Returns
        // false on evaluator failure so the caller can bail; success path always returns true,
        // even when whereClause is null or all rows are rejected.
        private static bool TryApplyWhereFilter(
            Node whereClause,
            List<JoinExecutor.JoinedRow> joinedRows,
            IReadOnlyList<JoinExecutor.SourceInfo> sources,
            RowScope outerScope,
            ExpressionEvaluator.ScalarSubqueryResolver subqueryResolver,
            ExpressionEvaluator.ScalarFunctionResolver functionResolver,
            out List<JoinExecutor.JoinedRow> filtered)
        {
            filtered = new List<JoinExecutor.JoinedRow>(joinedRows.Count);
            if (whereClause == null)
            {
                filtered.AddRange(joinedRows);
                return true;
            }
            foreach (var jr in joinedRows)
            {
                var scope = jr.ToScope(sources);
                if (outerScope != null)
                    scope = scope.WithParent(outerScope);
                if (ExpressionEvaluator.TryEvaluate(whereClause, scope, subqueryResolver, functionResolver, out var match) == false)
                    return false;
                if (ExpressionEvaluator.IsTruthy(match))
                    filtered.Add(jr);
            }
            return true;
        }

        // Must match what TryComputeAggregate actually implements. Widening this set without
        // also extending TryComputeAggregate makes the outer gate accept a shape the compute
        // step then bails on, surfacing a generic "Unhandled query".
        private static bool IsAggregateFunctionCall(FuncCall func)
        {
            // count(DISTINCT x) is not implemented by TryComputeAggregate (it would return the
            // non-distinct count - silently wrong). Fall through so it's diagnosed/translated elsewhere.
            if (func.AggDistinct)
                return false;
            if (func.AggStar)
                return true; // count(*)
            if (func.Funcname is not { Count: 1 })
                return false;
            var name = func.Funcname[0].String?.Sval;
            return string.Equals(name, "count", StringComparison.OrdinalIgnoreCase);
        }

        private static bool TryComputeAggregate(string aggName, FuncCall func, List<JoinExecutor.JoinedRow> rows,
                                                IReadOnlyList<JoinExecutor.SourceInfo> sources,
                                                RowScope outerScope,
                                                ExpressionEvaluator.ScalarSubqueryResolver subqueryResolver,
                                                ExpressionEvaluator.ScalarFunctionResolver functionResolver,
                                                out object value)
        {
            value = null;
            // Outer IsAggregateFunctionCall gate already restricts to count / count(*). When
            // extending to sum/min/max/avg, widen BOTH this guard and IsAggregateFunctionCall.
            if (string.Equals(aggName, "count", StringComparison.OrdinalIgnoreCase) == false && func.AggStar == false)
                return false;

            // count(*) - every filtered row counts.
            if (func.AggStar || func.Args == null || func.Args.Count == 0)
            {
                value = (long)rows.Count;
                return true;
            }

            // count(expr) - count rows where expr evaluates to non-null.
            long c = 0;
            foreach (var jr in rows)
            {
                var scope = jr.ToScope(sources);
                if (outerScope != null)
                    scope = scope.WithParent(outerScope);
                if (ExpressionEvaluator.TryEvaluate(func.Args[0], scope, subqueryResolver, functionResolver, out var v) == false)
                    return false;
                if (v != null)
                    c++;
            }
            value = c;
            return true;
        }

        // Runs the full pipeline but returns object[] rows + their schema instead of a serialized
        // PgTable. Used both by the top-level table-query path and recursively for sub-FROM bodies.
        private static bool TryExecuteAsRows(SelectStmt s, VirtualQueryContext ctx, out List<ProjectedTarget> columns, out List<object[]> rows)
            => TryExecuteAsRows(s, ctx, outerScope: null, out columns, out rows);

        private static bool TryExecuteAsRows(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out List<ProjectedTarget> columns, out List<object[]> rows)
        {
            columns = null;
            rows = null;

            if (RejectUnsupportedClauses(s))
                return false;

            // Pre-extract equality predicates from the WHERE so virtual tables (e.g.
            // information_schema.columns) can scope their enumeration. Save/restore around the
            // pipeline because sub-FROM resolution recurses through TryExecuteAsRows.
            var previousPredicates = ctx.Predicates;
            ctx.Predicates = ExtractEqualityPredicates(s.WhereClause);
            try
            {
                return TryExecuteAsRowsCore(s, ctx, outerScope, out columns, out rows);
            }
            finally
            {
                ctx.Predicates = previousPredicates;
            }
        }

        private static bool TryExecuteAsRowsCore(SelectStmt s, VirtualQueryContext ctx, RowScope outerScope, out List<ProjectedTarget> columns, out List<object[]> rows)
        {
            columns = null;
            rows = null;

            var executor = new JoinExecutor(ctx, (subselect, alias) => TryResolveSubquery(subselect, alias, ctx));
            if (executor.TryExecute(s.FromClause, out var joinedRows, out var sources) == false)
                return false;

            if (TryBuildProjection(s.TargetList, sources, out var projection) == false)
                return false;

            // Filter (WHERE).
            var subqueryResolver = MakeSubqueryResolver(ctx);
            var functionResolver = MakeFunctionResolver(ctx);

            if (TryApplyWhereFilter(s.WhereClause, joinedRows, sources, outerScope, subqueryResolver, functionResolver, out var filtered) == false)
                return false;

            // Sort plan: each entry either reuses a projected column or evaluates an expression
            // against the joined row alongside the projection (kept in a hidden tail of each row).
            if (TryBuildSortPlan(s, projection, out var sortPlan, out var extraSortExpressions) == false)
                return false;

            // Project (real columns first, then any expression-only sort keys appended).
            int totalCells = projection.Count + extraSortExpressions.Count;
            var projectedRows = new List<object[]>(filtered.Count);
            foreach (var jr in filtered)
            {
                var scope = jr.ToScope(sources);
                if (outerScope != null)
                    scope = scope.WithParent(outerScope);
                var cells = new object[totalCells];
                for (int i = 0; i < projection.Count; i++)
                {
                    if (ExpressionEvaluator.TryEvaluate(projection[i].Expression, scope, subqueryResolver, functionResolver, out var value) == false)
                        return false;
                    cells[i] = value;
                }
                for (int i = 0; i < extraSortExpressions.Count; i++)
                {
                    if (ExpressionEvaluator.TryEvaluate(extraSortExpressions[i], scope, subqueryResolver, functionResolver, out var sortValue) == false)
                        return false;
                    cells[projection.Count + i] = sortValue;
                }
                projectedRows.Add(cells);
            }

            if (sortPlan.Count > 0)
                projectedRows = SortProjectedRows(projectedRows, sortPlan);

            // Offset / limit.
            int offset = 0;
            int? limit = null;
            if (s.LimitOffset != null)
            {
                if (PgSqlAstHelpers.TryReadNonNegativeIntConst(s.LimitOffset, out offset) == false)
                    return false;
            }
            if (s.LimitCount != null)
            {
                if (PgSqlAstHelpers.TryReadNonNegativeIntConst(s.LimitCount, out var l) == false)
                    return false;
                limit = l;
            }
            if (offset > 0)
                projectedRows = projectedRows.Skip(offset).ToList();
            if (limit.HasValue)
                projectedRows = projectedRows.Take(limit.Value).ToList();

            columns = projection;
            rows = projectedRows;
            return true;
        }

        // Walks the WHERE tree and collects top-level ANDed `column = literal` equalities into a
        // dictionary keyed by the column's last-segment name (case-insensitive). Anything under OR
        // or NOT is ignored - those don't constrain the result set the same way and would mislead
        // virtual tables that use this for enumeration scoping.
        private static IReadOnlyDictionary<string, object> ExtractEqualityPredicates(Node whereClause)
        {
            var result = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            if (whereClause == null)
                return result;
            Collect(whereClause, result);
            return result;

            static void Collect(Node node, Dictionary<string, object> acc)
            {
                if (node == null)
                    return;

                // AND-expression: descend into every child (still equality-conjuncted at top level).
                if (node.BoolExpr is { Boolop: BoolExprType.AndExpr } andExpr)
                {
                    foreach (var arg in andExpr.Args)
                        Collect(arg, acc);
                    return;
                }

                // Single `column = literal` equality.
                var aExpr = node.AExpr;
                if (aExpr == null || aExpr.Kind != A_Expr_Kind.AexprOp)
                    return;
                if (aExpr.Name is not { Count: 1 } || aExpr.Name[0]?.String?.Sval != "=")
                    return;

                var fields = aExpr.Lexpr?.ColumnRef?.Fields;
                if (fields is not { Count: > 0 })
                    return;
                var columnName = fields[^1]?.String?.Sval;
                if (string.IsNullOrWhiteSpace(columnName))
                    return;

                var literal = aExpr.Rexpr?.AConst;
                if (literal == null)
                    return;

                object value = null;
                if (literal.Sval?.Sval != null) value = literal.Sval.Sval;
                else if (literal.Ival != null) value = (long)literal.Ival.Ival;
                else if (literal.Boolval != null) value = literal.Boolval.Boolval;

                if (value != null)
                    acc[columnName] = value;
            }
        }

        private static (IReadOnlyList<PgVirtualColumn> Columns, IReadOnlyList<object[]> Rows)? TryResolveSubquery(
            RangeSubselect subselect, string alias, VirtualQueryContext ctx)
        {
            var subStmt = subselect.Subquery?.SelectStmt;
            if (subStmt == null)
                return null;

            // Fast path: subqueries whose inner FROM sources are all always-empty don't need a
            // full pipeline run - only the column schema matters. This keeps the PowerBI
            // ReferentialConstraints / FkCentric metadata queries returning empty rowsets.
            if (TryDeriveEmptySubqueryColumns(subStmt, out var emptyColumns))
                return (emptyColumns, Array.Empty<object[]>());

            // Real path: recursively execute the inner SELECT into materialized rows.
            if (TryExecuteAsRows(subStmt, ctx, out var innerColumns, out var innerRows) == false)
                return null;

            var virtualColumns = new List<PgVirtualColumn>(innerColumns.Count);
            foreach (var c in innerColumns)
                virtualColumns.Add(new PgVirtualColumn(c.OutputName, c.PgType, c.FormatCode));
            return (virtualColumns, innerRows);
        }

        private static bool TryDeriveEmptySubqueryColumns(SelectStmt s, out IReadOnlyList<PgVirtualColumn> columns)
        {
            columns = null;

            // Inner sources must exist and be always-empty.
            if (s.FromClause is not { Count: > 0 } from)
                return false;

            var innerSources = new List<(string Alias, PgVirtualTable Table)>();
            foreach (var fromNode in from)
            {
                if (TryCollectInnerEmptySources(fromNode, innerSources) == false)
                    return false;
            }

            if (innerSources.Count == 0)
                return false;
            foreach (var src in innerSources)
            {
                if (src.Table.IsAlwaysEmpty == false)
                    return false;
            }

            if (s.TargetList is not { Count: > 0 } innerTargets)
                return false;

            var derived = new List<PgVirtualColumn>(innerTargets.Count);
            foreach (var target in innerTargets)
            {
                var rt = target?.ResTarget;
                if (rt == null)
                    return false;

                if (TryDeriveColumnFromInner(rt, innerSources, out var col) == false)
                    return false;
                derived.Add(col);
            }

            columns = derived;
            return true;
        }

        private static bool TryCollectInnerEmptySources(Node node, List<(string Alias, PgVirtualTable Table)> sources)
        {
            if (node == null) return false;
            if (node.RangeVar != null)
            {
                var rv = node.RangeVar;
                if (PgVirtualDatabase.TryGetTable(rv.Schemaname, rv.Relname, out var table) == false)
                    return false;
                sources.Add((rv.Alias?.Aliasname ?? rv.Relname, table));
                return true;
            }
            if (node.JoinExpr != null)
            {
                return TryCollectInnerEmptySources(node.JoinExpr.Larg, sources) &&
                       TryCollectInnerEmptySources(node.JoinExpr.Rarg, sources);
            }
            return false;
        }

        private static bool TryDeriveColumnFromInner(ResTarget rt, List<(string Alias, PgVirtualTable Table)> sources, out PgVirtualColumn col)
        {
            col = null;
            var val = rt.Val;
            if (val == null)
                return false;

            var colRef = val.ColumnRef;
            if (colRef?.Fields is { Count: > 0 } fields && fields[^1]?.AStar == null)
            {
                var columnName = fields[^1]?.String?.Sval;
                if (string.IsNullOrWhiteSpace(columnName))
                    return false;
                string qualifier = fields.Count >= 2 ? fields[^2]?.String?.Sval : null;

                foreach (var src in sources)
                {
                    if (qualifier != null && string.Equals(src.Alias, qualifier, StringComparison.OrdinalIgnoreCase) == false)
                        continue;
                    foreach (var c in src.Table.Columns)
                    {
                        if (string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
                        {
                            var outName = string.IsNullOrWhiteSpace(rt.Name) == false ? rt.Name : columnName;
                            col = new PgVirtualColumn(outName, c.PgType, c.FormatCode);
                            return true;
                        }
                    }
                    if (qualifier != null) return false;
                }
                return false;
            }

            // Expression-typed inner column needs an explicit alias.
            if (string.IsNullOrWhiteSpace(rt.Name))
                return false;

            var format = sources[0].Table.Columns.Count > 0 ? sources[0].Table.Columns[0].FormatCode : PgFormat.Text;
            col = new PgVirtualColumn(rt.Name, PgText.Default, format);
            return true;
        }

        // Projection planning
        private sealed record ProjectedTarget(Node Expression, string OutputName, PgType PgType, PgFormat FormatCode);

        private static bool TryBuildProjection(IList<Node> targetList, IReadOnlyList<JoinExecutor.SourceInfo> sources, out List<ProjectedTarget> projection)
        {
            projection = new List<ProjectedTarget>();
            if (targetList is not { Count: > 0 })
                return false;

            foreach (var target in targetList)
            {
                var rt = target?.ResTarget;
                if (rt == null)
                    return false;

                var val = rt.Val;
                if (val == null)
                    return false;

                var colRef = val.ColumnRef;
                if (colRef?.Fields is { Count: > 0 } fields)
                {
                    // Wildcard cases:
                    if (fields.Count == 1 && fields[0]?.AStar != null)
                    {
                        ExpandUnqualifiedWildcard(sources, projection);
                        continue;
                    }
                    if (fields.Count >= 2 && fields[^1]?.AStar != null)
                    {
                        var qualifier = fields[^2]?.String?.Sval;
                        if (ExpandQualifiedWildcard(qualifier, sources, projection) == false)
                            return false;
                        continue;
                    }

                    // Plain column ref.
                    var columnName = fields[^1]?.String?.Sval;
                    if (string.IsNullOrWhiteSpace(columnName))
                        return false;
                    string srcQualifier = fields.Count >= 2 ? fields[^2]?.String?.Sval : null;
                    if (TryResolveColumnType(srcQualifier, columnName, sources, out var pgType, out var format) == false)
                        return false;
                    var outName = string.IsNullOrWhiteSpace(rt.Name) == false ? rt.Name : columnName;
                    projection.Add(new ProjectedTarget(val, outName, pgType, format));
                    continue;
                }

                // Expression target - use the alias if given, otherwise PG's default "?column?".
                var (exprPgType, exprFormat) = InferExpressionType(val, sources);
                var exprName = string.IsNullOrWhiteSpace(rt.Name) == false ? rt.Name : "?column?";
                projection.Add(new ProjectedTarget(val, exprName, exprPgType, exprFormat));
            }

            return projection.Count > 0;
        }

        private static void ExpandUnqualifiedWildcard(IReadOnlyList<JoinExecutor.SourceInfo> sources, List<ProjectedTarget> projection)
        {
            foreach (var src in sources)
            {
                foreach (var c in src.Columns)
                {
                    var colRefNode = new Node { ColumnRef = new ColumnRef { Fields = { new Node { String = new PgSqlParser.String { Sval = src.Alias } }, new Node { String = new PgSqlParser.String { Sval = c.Name } } } } };
                    projection.Add(new ProjectedTarget(colRefNode, c.Name, c.PgType, c.FormatCode));
                }
            }
        }

        private static bool ExpandQualifiedWildcard(string qualifier, IReadOnlyList<JoinExecutor.SourceInfo> sources, List<ProjectedTarget> projection)
        {
            if (string.IsNullOrWhiteSpace(qualifier))
                return false;
            foreach (var src in sources)
            {
                if (string.Equals(src.Alias, qualifier, StringComparison.OrdinalIgnoreCase) == false)
                    continue;
                foreach (var c in src.Columns)
                {
                    var colRefNode = new Node
                    {
                        ColumnRef = new ColumnRef
                        {
                            Fields =
                            {
                                new Node { String = new PgSqlParser.String { Sval = src.Alias } },
                                new Node { String = new PgSqlParser.String { Sval = c.Name } }
                            }
                        }
                    };
                    projection.Add(new ProjectedTarget(colRefNode, c.Name, c.PgType, c.FormatCode));
                }
                return true;
            }
            return false;
        }

        private static bool TryResolveColumnType(string qualifier, string columnName, IReadOnlyList<JoinExecutor.SourceInfo> sources, out PgType pgType, out PgFormat format)
        {
            pgType = null;
            format = PgFormat.Text;

            foreach (var src in sources)
            {
                if (qualifier != null && string.Equals(src.Alias, qualifier, StringComparison.OrdinalIgnoreCase) == false)
                    continue;
                foreach (var c in src.Columns)
                {
                    if (string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        pgType = c.PgType;
                        format = c.FormatCode;
                        return true;
                    }
                }
                if (qualifier != null) return false;
            }
            return false;
        }

        // Heuristic: pick a sensible default PgType for arbitrary expressions. The format code is
        // inherited from the first source (all current catalog sources share a format).
        private static (PgType PgType, PgFormat Format) InferExpressionType(Node expr, IReadOnlyList<JoinExecutor.SourceInfo> sources)
        {
            var format = sources.Count > 0 && sources[0].Columns.Count > 0
                ? sources[0].Columns[0].FormatCode
                : PgFormat.Text;

            // AConst-only expression: derive type from the literal.
            if (expr.AConst != null)
                return (InferConstType(expr.AConst), format);

            // CASE WHEN: walk the WHEN/ELSE results and use the most-specific type.
            if (expr.CaseExpr != null)
                return (InferCaseType(expr.CaseExpr, sources), format);

            return (PgText.Default, format);
        }

        private static PgType InferConstType(A_Const c)
        {
            // Integer literals are evaluated as boxed long, so advertise Int8 to match.
            if (c.Ival != null) return PgInt8.Default;
            if (c.Boolval != null) return PgBool.Default;
            if (c.Fval != null) return PgFloat8.Default;
            // String literals - even single-character ones like 'Y' / 'N' - must be text.
            // PgChar (oid 18) is PG's internal `"char"` type (single byte, used in
            // pg_catalog rows like pg_type.typtype) and is only ever produced by an
            // explicit ::char cast. Typing a `case when ... then 'Y' else 'N' end`
            // result as PgChar breaks PowerBI's mashup engine inside RetrieveKeysForTable
            // when it decodes the PRIMARY_KEY column of our PK metadata join - the binary
            // single byte doesn't match its text-decoder contract and crashes with
            // `Nullable object must have a value` during the PK lookup that drives
            // SupportsPaging for every imported table.
            return PgText.Default;
        }

        private static PgType InferCaseType(CaseExpr caseExpr, IReadOnlyList<JoinExecutor.SourceInfo> sources)
        {
            // Prefer the first non-null result clause's type.
            if (caseExpr.Args != null)
            {
                foreach (var whenNode in caseExpr.Args)
                {
                    var when = whenNode?.CaseWhen;
                    if (when?.Result?.AConst != null)
                        return InferConstType(when.Result.AConst);
                    if (when?.Result?.ColumnRef?.Fields is { Count: > 0 } fields)
                    {
                        var name = fields[^1]?.String?.Sval;
                        string qual = fields.Count >= 2 ? fields[^2]?.String?.Sval : null;
                        if (name != null && TryResolveColumnType(qual, name, sources, out var pgType, out _))
                            return pgType;
                    }
                }
            }
            if (caseExpr.Defresult?.AConst != null)
                return InferConstType(caseExpr.Defresult.AConst);
            return PgText.Default;
        }

        // ORDER BY plan. Each entry either reuses a projected column (sort key matches an output
        // alias) or is evaluated as an expression against the joined row and appended to a hidden
        // tail of each projected row. `SortPlan` indexes into the full per-row cell array.
        private readonly record struct SortKey(int CellIndex, bool Descending);

        private static bool TryBuildSortPlan(SelectStmt s, List<ProjectedTarget> projection, out List<SortKey> plan, out List<Node> extraExpressions)
        {
            plan = new List<SortKey>();
            extraExpressions = new List<Node>();
            if (s.SortClause is not { Count: > 0 } sort)
                return true;

            foreach (var sortNode in sort)
            {
                var sortBy = sortNode?.SortBy;
                if (sortBy?.Node == null)
                    return false;

                // First check: does the sort key reference a projected output alias?
                int projectedIdx = -1;
                var colRef = sortBy.Node.ColumnRef;
                if (colRef?.Fields is { Count: 1 } fields)
                {
                    var name = fields[0]?.String?.Sval;
                    for (int i = 0; i < projection.Count; i++)
                    {
                        if (string.Equals(projection[i].OutputName, name, StringComparison.OrdinalIgnoreCase))
                        {
                            projectedIdx = i;
                            break;
                        }
                    }
                }

                bool descending = sortBy.SortbyDir == SortByDir.SortbyDesc;
                if (projectedIdx >= 0)
                {
                    plan.Add(new SortKey(projectedIdx, descending));
                }
                else
                {
                    // Evaluate the expression alongside the projection.
                    extraExpressions.Add(sortBy.Node);
                    plan.Add(new SortKey(projection.Count + extraExpressions.Count - 1, descending));
                }
            }
            return true;
        }

        private static List<object[]> SortProjectedRows(List<object[]> rows, List<SortKey> plan)
        {
            rows.Sort((a, b) =>
            {
                foreach (var key in plan)
                {
                    var cmp = CompareCells(a[key.CellIndex], b[key.CellIndex]);
                    if (cmp != 0)
                        return key.Descending ? -cmp : cmp;
                }
                return 0;
            });
            return rows;
        }

        // ORDER BY comparator. Must give a consistent ordering for EVERY pair of values, or List.Sort
        // throws. The catch: a column can hold mixed types (e.g. a CASE that returns a number in one row
        // and a string in another), and comparing those without care can contradict itself - say a < b
        // and b < c but c < a - which is what makes List.Sort throw. To stay consistent we first order by
        // type group (null < number < bool < other); only values in the SAME group are compared directly,
        // and always the same way: numbers numerically, everything else as an ordinal string.
        private static int CompareCells(object a, object b)
        {
            var (rankA, numA) = Classify(a);
            var (rankB, numB) = Classify(b);
            if (rankA != rankB)
                return rankA.CompareTo(rankB);

            return rankA switch
            {
                1 => numA.CompareTo(numB),                              // both numeric (incl. numeric strings)
                2 => ((bool)a).CompareTo((bool)b),                      // both bool
                3 => string.CompareOrdinal(a.ToString(), b.ToString()), // both other - one consistent rule
                _ => 0                                                  // both null
            };
        }

        // Category rank plus, for numbers, the coerced value. Numeric strings sort as numbers (matching
        // ExpressionEvaluator's coercion); everything else compares as a string.
        private static (int Rank, double Num) Classify(object v)
        {
            if (v is null) return (0, 0);
            if (v is bool) return (2, 0);
            if (TryToDouble(v, out var d)) return (1, d);
            return (3, 0);
        }

        private static bool TryToDouble(object v, out double d)
        {
            switch (v)
            {
                case double dd: d = dd; return true;
                case float f: d = f; return true;
                case long l: d = l; return true;
                case int i: d = i; return true;
                case short s: d = s; return true;
                case string str when double.TryParse(str, NumberStyles.Float, CultureInfo.InvariantCulture, out var p):
                    d = p; return true;
                default: d = 0; return false;
            }
        }

        private static PgTable BuildPgTable(List<ProjectedTarget> projection, List<object[]> rows)
        {
            var pgColumns = new List<PgColumn>(projection.Count);
            for (int i = 0; i < projection.Count; i++)
            {
                pgColumns.Add(new PgColumn(projection[i].OutputName, columnIndex: (short)i, pgType: projection[i].PgType, formatCode: projection[i].FormatCode));
            }

            var data = new List<PgDataRow>(rows.Count);
            foreach (var row in rows)
            {
                var cells = new ReadOnlyMemory<byte>?[projection.Count];
                for (int i = 0; i < projection.Count; i++)
                {
                    var value = row[i];
                    if (value == null)
                        cells[i] = null;
                    else
                        cells[i] = projection[i].PgType.ToBytes(value, projection[i].FormatCode);
                }
                data.Add(new PgDataRow(cells));
            }
            return new PgTable { Columns = pgColumns, Data = data };
        }
    }
}
