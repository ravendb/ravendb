using System;
using System.Collections.Generic;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    // Materializes a SELECT's FROM clause into a sequence of "joined rows" - flat arrays of
    // per-source rows. Supports:
    //   - Base-table FROM sources (PgVirtualTable resolved via PgVirtualDatabase) and sub-SELECT
    //     sources (resolved by an injected callback).
    //   - INNER and LEFT OUTER joins with a single equality ON condition (a.x = b.y) or a trivial
    //     `1=1`/`true` predicate (treated as a cross product).
    //   - Implicit cross product when multiple sources appear in the top-level FROM list.
    //
    // Bails (returns false) on shapes outside that envelope - caller falls through to the next
    // dispatch tier just like every other interpreter step.
    internal sealed class JoinExecutor
    {
        public readonly record struct SourceInfo(string Alias, IReadOnlyList<PgVirtualColumn> Columns);

        // A joined row: parallel arrays. row[i] is the row contributed by sources[i]; row[i] is
        // null when this source was outer-joined and unmatched.
        public sealed class JoinedRow
        {
            public readonly object[][] Rows;
            public JoinedRow(object[][] rows) { Rows = rows; }

            public RowScope ToScope(IReadOnlyList<SourceInfo> sources)
            {
                var b = RowScope.Builder();
                for (int i = 0; i < sources.Count; i++)
                    b.Add(sources[i].Alias, sources[i].Columns, Rows[i]);
                return b.Build();
            }
        }

        private sealed record SourceState(string Alias, IReadOnlyList<PgVirtualColumn> Columns, IReadOnlyList<object[]> Rows);

        private abstract record JoinKind;
        private sealed record InnerJoin(JoinCondition Condition) : JoinKind;
        private sealed record LeftOuterJoin(JoinCondition Condition) : JoinKind;
        private sealed record CartesianProduct : JoinKind;

        private abstract record JoinCondition;
        private sealed record EqualityCondition(IReadOnlyList<string> LeftPath, IReadOnlyList<string> RightPath) : JoinCondition;
        private sealed record AndCondition(IReadOnlyList<EqualityCondition> Equalities) : JoinCondition;
        private sealed record TrivialTrueCondition : JoinCondition;

        private sealed record JoinStep(SourceState Right, JoinKind Kind);

        private readonly VirtualQueryContext _ctx;
        private readonly Func<RangeSubselect, string, (IReadOnlyList<PgVirtualColumn> Columns, IReadOnlyList<object[]> Rows)?> _subqueryResolver;

        public JoinExecutor(VirtualQueryContext ctx,
                            Func<RangeSubselect, string, (IReadOnlyList<PgVirtualColumn> Columns, IReadOnlyList<object[]> Rows)?> subqueryResolver = null)
        {
            _ctx = ctx;
            _subqueryResolver = subqueryResolver;
        }

        public bool TryExecute(IList<Node> fromClause, out List<JoinedRow> rows, out IReadOnlyList<SourceInfo> sources)
        {
            rows = null;
            sources = null;
            if (fromClause is not { Count: > 0 })
                return false;

            var initialSources = new List<SourceState>();
            var joinSteps = new List<JoinStep>();

            foreach (var fromNode in fromClause)
            {
                if (TryFlattenFromNode(fromNode, initialSources, joinSteps) == false)
                    return false;
            }

            if (initialSources.Count == 0)
                return false;

            // Cartesian product of the initial sources.
            var seeded = SeedCartesian(initialSources);
            if (seeded == null)
                return false; // oversized product - bail to fall-through instead of OOM

            int sourceCount = initialSources.Count + joinSteps.Count;
            var publicSources = new List<SourceInfo>(sourceCount);
            foreach (var s in initialSources)
                publicSources.Add(new SourceInfo(s.Alias, s.Columns));

            // Apply each join step in order.
            int seenSources = initialSources.Count;
            foreach (var step in joinSteps)
            {
                publicSources.Add(new SourceInfo(step.Right.Alias, step.Right.Columns));
                seeded = ApplyJoin(seeded, publicSources, seenSources, step, sourceCount);
                if (seeded == null)
                    return false;
                seenSources++;
            }

            // For steps that haven't run yet, ensure the rows array has the full width.
            rows = seeded;
            sources = publicSources;
            return true;
        }

        private bool TryFlattenFromNode(Node node, List<SourceState> initialSources, List<JoinStep> joinSteps)
        {
            if (node == null)
                return false;

            if (node.JoinExpr != null)
            {
                var join = node.JoinExpr;

                if (TryFlattenFromNode(join.Larg, initialSources, joinSteps) == false)
                    return false;

                if (TryResolveSource(join.Rarg, out var rightSrc) == false)
                    return false;

                if (TryParseOnCondition(join.Quals, out var condition) == false)
                    return false;

                JoinKind kind = join.Jointype switch
                {
                    JoinType.JoinInner => condition is TrivialTrueCondition ? new CartesianProduct() : new InnerJoin(condition),
                    JoinType.JoinLeft  => new LeftOuterJoin(condition),
                    _ => null
                };
                if (kind == null)
                    return false;

                joinSteps.Add(new JoinStep(rightSrc, kind));
                return true;
            }

            if (TryResolveSource(node, out var src))
            {
                initialSources.Add(src);
                return true;
            }
            return false;
        }

        private bool TryResolveSource(Node node, out SourceState source)
        {
            source = null;
            if (node == null)
                return false;

            if (node.RangeVar != null)
            {
                var rv = node.RangeVar;
                var alias = rv.Alias?.Aliasname ?? rv.Relname;

                // Check enclosing WITH CTEs first - a `FROM cte` reference inside a WITH RECURSIVE
                // recursive case should resolve to the in-progress CTE, not a real catalog table.
                if (string.IsNullOrEmpty(rv.Schemaname) && _ctx?.Ctes != null
                    && _ctx.Ctes.TryGetValue(rv.Relname, out var cte))
                {
                    source = new SourceState(alias, cte.Columns, cte.Rows);
                    return true;
                }

                if (PgVirtualDatabase.TryGetTable(rv.Schemaname, rv.Relname, out var table) == false)
                    return false;
                source = new SourceState(alias, table.Columns, MaterializeRows(table));
                return true;
            }

            if (node.RangeSubselect != null && _subqueryResolver != null)
            {
                var alias = node.RangeSubselect.Alias?.Aliasname;
                if (string.IsNullOrWhiteSpace(alias))
                    return false;
                var resolved = _subqueryResolver(node.RangeSubselect, alias);
                if (resolved == null)
                    return false;
                source = new SourceState(alias, resolved.Value.Columns, resolved.Value.Rows);
                return true;
            }

            return false;
        }

        private IReadOnlyList<object[]> MaterializeRows(PgVirtualTable table)
        {
            if (table.IsAlwaysEmpty)
                return Array.Empty<object[]>();
            var list = new List<object[]>();
            foreach (var row in table.EnumerateRows(_ctx))
                list.Add(row);
            return list;
        }

        private static bool TryParseOnCondition(Node quals, out JoinCondition condition)
        {
            condition = null;
            if (quals == null)
            {
                condition = new TrivialTrueCondition();
                return true;
            }

            if (IsTrivialTrue(quals))
            {
                condition = new TrivialTrueCondition();
                return true;
            }

            // Compound AND. Each arm is either:
            //   - a column-to-column equality (PowerBI ReferentialConstraints: fkcon.X=fkcol.X
            //     AND fkcon.Y=fkcol.Y) - kept as a join key.
            //   - a column-to-literal equality (pgAdmin: `descr.classoid='pg_database'::regclass`)
            //     - these are post-join row filters in real PG. We skip them here; for LEFT-JOINed
            //     empty right tables (the common pgAdmin case) the missing filter doesn't change
            //     the result, and the WHERE clause typically re-applies any meaningful constraint.
            // If every arm is non-equality, we degrade to a trivial-true condition (cross/all-row
            // join) rather than failing the whole dispatch.
            if (quals.BoolExpr is { Boolop: BoolExprType.AndExpr } andExpr && andExpr.Args is { Count: > 0 })
            {
                var equalities = new List<EqualityCondition>(andExpr.Args.Count);
                foreach (var arg in andExpr.Args)
                {
                    if (TryParseEqualityNode(arg, out var eq))
                        equalities.Add(eq);
                    // else: column-to-literal or constant predicate - silently dropped.
                }
                condition = equalities.Count > 0
                    ? new AndCondition(equalities)
                    : new TrivialTrueCondition();
                return true;
            }

            if (TryParseEqualityNode(quals, out var single))
            {
                condition = single;
                return true;
            }

            return false;
        }

        private static bool TryParseEqualityNode(Node node, out EqualityCondition condition)
        {
            condition = null;
            var aExpr = node?.AExpr;
            if (aExpr == null || aExpr.Name is not { Count: 1 })
                return false;
            var op = aExpr.Name[0]?.String?.Sval;
            if (op != "=")
                return false;

            if (TryExtractColumnPath(aExpr.Lexpr, out var leftPath) == false)
                return false;
            if (TryExtractColumnPath(aExpr.Rexpr, out var rightPath) == false)
                return false;

            condition = new EqualityCondition(leftPath, rightPath);
            return true;
        }

        private static bool TryExtractColumnPath(Node node, out IReadOnlyList<string> path)
        {
            path = null;
            var fields = node?.ColumnRef?.Fields;
            if (fields == null || fields.Count == 0)
                return false;

            var segments = new List<string>(fields.Count);
            foreach (var f in fields)
            {
                var s = f?.String?.Sval;
                if (string.IsNullOrWhiteSpace(s))
                    return false;
                segments.Add(s);
            }
            path = segments;
            return true;
        }

        private static bool IsTrivialTrue(Node node)
        {
            if (node.AConst?.Boolval != null)
                return node.AConst.Boolval.Boolval;

            var ae = node.AExpr;
            if (ae?.Name is { Count: 1 } name && name[0]?.String?.Sval == "=" &&
                ae.Lexpr?.AConst?.Ival != null && ae.Rexpr?.AConst?.Ival != null)
            {
                return ae.Lexpr.AConst.Ival.Ival == ae.Rexpr.AConst.Ival.Ival;
            }

            return false;
        }

        // Cap on intermediate joined-row count. A legitimate pg_catalog / information_schema probe
        // never produces a large product; refusing to materialize beyond this turns a client-driven
        // blow-up (e.g. SELECT count(*) FROM pg_type a, pg_type b, pg_type c, pg_type d) into a
        // graceful fall-through instead of multi-GB allocation inside the shared server process.
        private const int MaxJoinedRows = 100_000;

        // Returns null when the product would exceed MaxJoinedRows (the caller bails to fall-through).
        private static List<JoinedRow> SeedCartesian(List<SourceState> sources)
        {
            // Start with a single empty row, then expand cartesian-style for each initial source.
            var result = new List<JoinedRow> { new(new object[sources.Count][]) };

            for (int i = 0; i < sources.Count; i++)
            {
                var src = sources[i];
                if (src.Rows.Count == 0)
                    return new List<JoinedRow>(); // empty source -> empty result (matches INNER semantics)

                if ((long)result.Count * src.Rows.Count > MaxJoinedRows)
                    return null;

                var next = new List<JoinedRow>(result.Count * src.Rows.Count);
                foreach (var jr in result)
                {
                    foreach (var row in src.Rows)
                    {
                        var copy = new object[sources.Count][];
                        Array.Copy(jr.Rows, copy, sources.Count);
                        copy[i] = row;
                        next.Add(new JoinedRow(copy));
                    }
                }
                result = next;
            }
            return result;
        }

        private static List<JoinedRow> ApplyJoin(List<JoinedRow> left, IReadOnlyList<SourceInfo> sourcesSoFarPlusRight, int rightIndex, JoinStep step, int totalSources)
        {
            // sourcesSoFarPlusRight has rightIndex+1 entries (we already appended `step.Right`).
            // Each left row has totalSources slots - only slots [0..rightIndex-1] are populated.
            var rightRows = step.Right.Rows;

            var output = new List<JoinedRow>();

            // Build a single hash index when the ON predicate is a single equality. Compound AND
            // conditions fall back to a per-row scan checking each equality (small N for catalog
            // tables - not worth a composite-key index here).
            //
            // Normalize orientation: the parser writes the ON as the user did (e.g. `ns.oid = a.x`
            // where `ns` is the right source). We re-orient so `Left` always refers to a previously
            // joined source and `Right` to the new right source.
            Dictionary<object, List<object[]>> rightIndexLookup = null;
            EqualityCondition singleEquality = NormalizeEqualityOrientation(step.Right.Alias, ExtractSingleEqualityForIndex(step.Kind));
            if (singleEquality != null)
                rightIndexLookup = BuildRightIndex(step.Right, singleEquality.RightPath, out _);

            foreach (var leftRow in left)
            {
                IEnumerable<object[]> matches = step.Kind switch
                {
                    CartesianProduct                                        => rightRows,
                    InnerJoin { Condition: TrivialTrueCondition }            => rightRows,
                    LeftOuterJoin { Condition: TrivialTrueCondition }        => rightRows,
                    InnerJoin { Condition: EqualityCondition }              => LookupMatches(leftRow, sourcesSoFarPlusRight, rightIndex, singleEquality, rightIndexLookup),
                    InnerJoin { Condition: AndCondition ac }                 => ScanCompound(leftRow, sourcesSoFarPlusRight, rightIndex, step.Right, NormalizeCompoundOrientation(step.Right.Alias, ac)),
                    LeftOuterJoin { Condition: EqualityCondition }          => LookupMatches(leftRow, sourcesSoFarPlusRight, rightIndex, singleEquality, rightIndexLookup),
                    LeftOuterJoin { Condition: AndCondition ac }             => ScanCompound(leftRow, sourcesSoFarPlusRight, rightIndex, step.Right, NormalizeCompoundOrientation(step.Right.Alias, ac)),
                    _ => null
                };

                if (matches == null)
                    continue;

                var isLeftOuter = step.Kind is LeftOuterJoin;
                var any = false;
                foreach (var rr in matches)
                {
                    any = true;
                    output.Add(MergeRight(leftRow, rr, rightIndex, totalSources));
                    if (output.Count > MaxJoinedRows)
                        return null; // oversized product - bail to fall-through
                }
                if (any == false && isLeftOuter)
                    output.Add(MergeRight(leftRow, null, rightIndex, totalSources));
            }

            return output;
        }

        private static EqualityCondition ExtractSingleEqualityForIndex(JoinKind kind)
        {
            return kind switch
            {
                InnerJoin { Condition: EqualityCondition ec }      => ec,
                LeftOuterJoin { Condition: EqualityCondition ec }  => ec,
                _ => null
            };
        }

        // If `eq.RightPath` doesn't reference `rightAlias` but `eq.LeftPath` does, swap them so the
        // hash index can always be keyed off the right source's column.
        private static EqualityCondition NormalizeEqualityOrientation(string rightAlias, EqualityCondition eq)
        {
            if (eq == null)
                return null;

            var rightFirst = eq.RightPath.Count >= 2 ? eq.RightPath[0] : null;
            if (string.Equals(rightFirst, rightAlias, StringComparison.OrdinalIgnoreCase))
                return eq;

            var leftFirst = eq.LeftPath.Count >= 2 ? eq.LeftPath[0] : null;
            if (string.Equals(leftFirst, rightAlias, StringComparison.OrdinalIgnoreCase))
                return new EqualityCondition(eq.RightPath, eq.LeftPath);

            return eq;
        }

        private static AndCondition NormalizeCompoundOrientation(string rightAlias, AndCondition ac)
        {
            var normalized = new List<EqualityCondition>(ac.Equalities.Count);
            foreach (var eq in ac.Equalities)
                normalized.Add(NormalizeEqualityOrientation(rightAlias, eq));
            return new AndCondition(normalized);
        }

        private static List<object[]> ScanCompound(JoinedRow leftRow, IReadOnlyList<SourceInfo> sources, int rightIndex, SourceState right, AndCondition condition)
        {
            // Compound AND condition: walk every right row, check every equality.
            var matches = new List<object[]>();
            foreach (var rr in right.Rows)
            {
                var ok = true;
                foreach (var eq in condition.Equalities)
                {
                    var leftVal = ResolveValueFromLeftRow(leftRow, sources, rightIndex, eq.LeftPath);
                    var rightVal = ResolveValueFromRightRow(rr, right, eq.RightPath);
                    // Both have to be non-null AND equal for the predicate to hold.
                    if (NormalizeKey(leftVal) == null || NormalizeKey(rightVal) == null || NormalizeKey(leftVal).Equals(NormalizeKey(rightVal)) == false)
                    {
                        ok = false;
                        break;
                    }
                }
                if (ok)
                    matches.Add(rr);
            }
            return matches;
        }

        private static object ResolveValueFromRightRow(object[] row, SourceState right, IReadOnlyList<string> path)
        {
            var name = path[^1];
            for (int i = 0; i < right.Columns.Count; i++)
            {
                if (string.Equals(right.Columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    return row?[i];
            }
            return null;
        }

        private static IEnumerable<object[]> LookupMatches(
            JoinedRow leftRow,
            IReadOnlyList<SourceInfo> sources,
            int rightIndex,
            EqualityCondition condition,
            Dictionary<object, List<object[]>> rightIndexLookup)
        {
            if (condition == null || rightIndexLookup == null)
                return Array.Empty<object[]>();

            // Condition is normalized so LeftPath refers to a previously joined source.
            var leftValue = ResolveValueFromLeftRow(leftRow, sources, rightIndex, condition.LeftPath);
            if (leftValue == null)
                return Array.Empty<object[]>();

            return rightIndexLookup.TryGetValue(NormalizeKey(leftValue), out var bucket)
                ? bucket
                : Array.Empty<object[]>();
        }

        private static object ResolveValueFromLeftRow(JoinedRow leftRow, IReadOnlyList<SourceInfo> sources, int rightIndex, IReadOnlyList<string> path)
        {
            // We may be given an `alias.column` path that resolves to the right-side source we're
            // currently joining in - flip the paths in that case by attempting both orientations.
            // For the simple case, look up left-side first.
            var name = path[^1];
            string qualifier = path.Count >= 2 ? path[^2] : null;

            for (int i = 0; i < rightIndex; i++) // only sources before the new one
            {
                if (qualifier != null &&
                    string.Equals(sources[i].Alias, qualifier, StringComparison.OrdinalIgnoreCase) == false)
                    continue;
                var cols = sources[i].Columns;
                for (int c = 0; c < cols.Count; c++)
                {
                    if (string.Equals(cols[c].Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        return leftRow.Rows[i]?[c];
                    }
                }
                if (qualifier != null)
                    return null;
            }

            return null;
        }

        private static Dictionary<object, List<object[]>> BuildRightIndex(SourceState rightSrc, IReadOnlyList<string> rightPath, out string rightColumnName)
        {
            rightColumnName = rightPath[^1];
            int colIndex = -1;
            for (int i = 0; i < rightSrc.Columns.Count; i++)
            {
                if (string.Equals(rightSrc.Columns[i].Name, rightColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    colIndex = i;
                    break;
                }
            }
            if (colIndex < 0)
                return new Dictionary<object, List<object[]>>();

            var index = new Dictionary<object, List<object[]>>();
            foreach (var row in rightSrc.Rows)
            {
                var key = NormalizeKey(row?[colIndex]);
                if (key == null) continue; // NULL keys never match anything per SQL semantics.
                if (index.TryGetValue(key, out var bucket) == false)
                {
                    bucket = new List<object[]>();
                    index[key] = bucket;
                }
                bucket.Add(row);
            }
            return index;
        }

        private static object NormalizeKey(object value)
        {
            if (value == null)
                return null;
            // Collapse numeric types to long where possible for stable equality.
            if (value is int i) return (long)i;
            if (value is short s) return (long)s;
            if (value is byte b) return (long)b;
            return value;
        }

        private static JoinedRow MergeRight(JoinedRow leftRow, object[] rightRow, int rightIndex, int totalSources)
        {
            var rows = new object[totalSources][];
            Array.Copy(leftRow.Rows, rows, rightIndex);
            rows[rightIndex] = rightRow;
            return new JoinedRow(rows);
        }
    }
}
