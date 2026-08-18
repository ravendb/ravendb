using System;
using System.Collections.Generic;
using PgSqlParser;
using Raven.Server.Integrations.PostgreSQL.Translation;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;

namespace Raven.Server.Integrations.PostgreSQL
{
    // Classifies SQL shapes the server can't execute, so the wire error says WHY instead of a generic
    // "Unhandled query: <SQL dump>". Consulted only after every TryParse dispatch arm returned false
    // (never in the hot path). These are known RavenDB PG-bridge limitations, not bugs; anything not
    // recognized here stays a generic "Unhandled query" so we don't mislabel a fixable bug.
    internal static class UnhandledQueryDiagnoser
    {
        public static bool TryDiagnose(string queryText, out string message)
        {
            message = null;
            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            // PowerBI's connector splits the query on `;` client-side, so an RQL `declare function
            // {...;...}` body arrives as just its leading fragment (unbalanced `{`), which won't parse.
            // Catch it before the parser-based checks and tell the user to remove the semicolons.
            if (LooksLikeJsBodyFragment(queryText))
            {
                message = "The query content looks like a fragment of a `declare function {...}` body. Some PostgreSQL clients split queries on `;` before sending, so only the first piece reaches the server. Remove the semicolons from the JS body.";
                return true;
            }

            SelectStmt outer;
            try
            {
                var parseResult = SqlAstCache.GetOrParse(queryText);
                if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: 1 })
                    return false;
                outer = parseResult.Value.Stmts[0]?.Stmt?.SelectStmt;
            }
            catch
            {
                // Parser failed on a shape we don't recognize; let the generic catch-all handle it.
                return false;
            }

            if (outer == null)
                return false;

            if (HasIntersectOrExcept(outer))
            {
                message = "SQL INTERSECT / EXCEPT are not supported. The PG-bridge virtual catalog implements UNION / UNION ALL only — express the intent with a different combination (e.g. UNION of conditions) or compute the result client-side.";
                return true;
            }

            if (HasJoinExpr(outer))
            {
                // A JOIN whose relations are all pg_catalog / information_schema tables is a
                // system-catalog probe from a BI/SQL client (e.g. SQLAlchemy's hstore lookup),
                // NOT a user query over RavenDB collections. Labeling it as the latter is
                // misleading and sends people chasing an RQL rewrite for a query they never wrote.
                message = AllJoinRelationsAreCatalogTables(outer)
                    ? "This system-catalog JOIN shape is not yet supported by the PostgreSQL-bridge virtual catalog. The query targets pg_catalog / information_schema tables (not RavenDB collections) and is typically emitted by a BI/SQL client during connection setup. Please report the exact query so the shape can be added."
                    : "SQL JOIN over RavenDB collections is not supported. RavenDB models cross-document relationships via document IDs rather than relational joins — express the relationship in RQL using `load` / `include`, or denormalize the data into the parent document.";
                return true;
            }

            if (HasMinOrMaxAggregate(outer))
            {
                message = "min() and max() aggregates are not supported by RavenDB's map-reduce engine — its AggregationOperation set is limited to Count and Sum. To get the minimum / maximum of a field, do `ORDER BY <field> ASC LIMIT 1` (min) or `ORDER BY <field> DESC LIMIT 1` (max) and read the single value client-side.";
                return true;
            }

            if (HasAvgAggregate(outer))
            {
                message = "avg() is not supported by RavenDB's map-reduce engine — its AggregationOperation set is limited to Count and Sum. Compute the average client-side: select sum(<field>) and count(*) and divide.";
                return true;
            }

            if (TryGetUnsupportedAggregateModifier(outer, out var modifier))
            {
                message = $"An aggregate with {modifier} is not supported. RavenDB's map-reduce aggregates every row that falls into a group, so there is no way to de-duplicate, pre-filter or window the values being aggregated — `count(DISTINCT x)`, `sum(DISTINCT x)`, `count(*) FILTER (WHERE ...)` and `count(*) OVER (...)` would all silently return the plain count/sum over the whole group. De-duplicate or filter client-side, or group by the column so each group already holds only the values you want.";
                return true;
            }

            if (HasCountOverColumn(outer))
            {
                message = "count(<column>) is not supported — only count(*). PostgreSQL counts the non-NULL values of the named column, while RavenDB's count() returns the group's total row count and ignores the argument, so the two disagree for every document that is missing the field. Use count(*) for the row count, or compute the non-null count client-side.";
                return true;
            }

            // Before the scalar-aggregate check: these predicates make `SELECT count(*) ... WHERE x IS
            // DISTINCT FROM y` fail translation, and blaming the aggregate would hide the real cause.
            if (HasDistinctFromPredicate(outer))
            {
                message = "IS DISTINCT FROM / IS NOT DISTINCT FROM (and NULLIF used as a predicate) are not supported. PostgreSQL builds them from a plain `=` operator that differs only in how it treats NULL, and RavenDB does not distinguish a stored null from a missing field, so neither form can be translated without changing which documents match. Combine `=` / `!=` with an explicit `IS NULL` / `IS NOT NULL` check, or run the query as RQL.";
                return true;
            }

            if (TryGetUnsupportedSortModifier(outer, out var sortModifier))
            {
                message = $"ORDER BY ... {sortModifier} is not supported. RavenDB orders missing and null values by its own rule, which is not guaranteed to match PostgreSQL's, and RQL cannot express a per-key null placement or a sort driven by an operator — so the clause is rejected rather than silently sorted a different way. Drop the clause if the default ordering will do, or sort the rows client-side.";
                return true;
            }

            // count(*) on its own translates, so when a scalar count(*) query still failed the aggregate
            // is not the cause — blame the WHERE clause when that is what the translator choked on.
            if (IsSupportedScalarCountStar(outer) && TryDiagnoseWhereClause(outer, out message))
                return true;

            if (IsScalarAggregateWithoutGroupBy(outer))
            {
                message = "Scalar aggregate without GROUP BY is supported for `count(*)` only. RavenDB's sum() is a map-reduce aggregation that requires a GROUP BY, so `SELECT sum(...) FROM t` with no grouping has no RQL form — compute the aggregate client-side from the underlying rows.";
                return true;
            }

            // Before the GROUP BY checks: an unsupported LIKE on a group key otherwise gets reported as a
            // non-grouped-field filter, which points the user at the wrong part of the query.
            if (TryDiagnoseLike(outer, out message))
                return true;

            if (outer.HavingClause != null)
            {
                message = "SQL HAVING is not supported. RavenDB filters aggregated groups with a post-reduction predicate the bridge doesn't translate yet — fetch the grouped rows without HAVING and apply the threshold client-side.";
                return true;
            }

            // A WHERE here alongside GROUP BY is a non-key filter (a group-key WHERE translates fine and
            // never reaches this point); RavenDB applies WHERE post-reduction, so it would change the aggregates.
            if (outer.GroupClause is { Count: > 0 } && outer.WhereClause != null)
            {
                message = "A WHERE on a non-grouped field can't be combined with GROUP BY: RavenDB's map-reduce applies WHERE to the aggregated result, not the source rows, so the filter would silently change the aggregates. Filter only on a GROUP BY key, or pre-filter the data (e.g. via a dedicated index) before aggregating.";
                return true;
            }

            return false;
        }

        // Reports the first LIKE / ILIKE whose pattern shape has no correct RQL form. Supported shapes
        // return false so a query that failed for an unrelated reason keeps its own diagnosis.
        private static bool TryDiagnoseLike(SelectStmt selectStmt, out string message)
        {
            message = null;

            var wheres = new List<Node>();
            CollectWhereClauses(selectStmt, wheres, depth: 0);

            foreach (var where in wheres)
            {
                if (TryDiagnoseLike(where, depth: 0, out message))
                    return true;
            }

            return false;
        }

        private static void CollectWhereClauses(SelectStmt selectStmt, List<Node> acc, int depth)
        {
            if (selectStmt == null || depth >= MaxJoinSearchDepth)
                return;

            if (selectStmt.Op != SetOperation.SetopNone)
            {
                CollectWhereClauses(selectStmt.Larg, acc, depth + 1);
                CollectWhereClauses(selectStmt.Rarg, acc, depth + 1);
            }

            if (selectStmt.WhereClause != null)
                acc.Add(selectStmt.WhereClause);

            if (selectStmt.FromClause == null)
                return;

            foreach (var item in selectStmt.FromClause)
            {
                if (item?.RangeSubselect?.Subquery?.SelectStmt is { } inner)
                    CollectWhereClauses(inner, acc, depth + 1);
            }
        }

        private static bool TryDiagnoseLike(Node node, int depth, out string message)
        {
            message = null;
            if (node == null || depth >= MaxJoinSearchDepth)
                return false;

            if (node.BoolExpr?.Args is { } args)
            {
                foreach (var arg in args)
                {
                    if (TryDiagnoseLike(arg, depth + 1, out message))
                        return true;
                }
                return false;
            }

            if (node.AExpr is not { } aExpr)
                return false;

            var op = aExpr.Name is { Count: 1 } ? aExpr.Name[0]?.String?.Sval?.Trim() : null;
            if (op != null && SqlLikePattern.IsLikeOperator(op))
            {
                var pattern = aExpr.Rexpr?.AConst?.Sval?.Sval;
                if (pattern == null)
                {
                    message = SqlLikePattern.NonLiteralPattern;
                    return true;
                }

                return SqlLikePattern.TryClassify(pattern, out _, out _, out message) == false;
            }

            return TryDiagnoseLike(aExpr.Lexpr, depth + 1, out message)
                || TryDiagnoseLike(aExpr.Rexpr, depth + 1, out message);
        }

        // Textual check for a `declare function {...}` fragment: starts with `declare function` and has
        // unbalanced `{` (the client cut it off at a `;`). Runs before AST checks since the fragment
        // won't parse. Brace counting skips quoted regions (so `return "}"` can't balance a real brace)
        // but not `--` / `/* */` comments - the body is JS, where `i--` is common.
        private static bool LooksLikeJsBodyFragment(string queryText)
        {
            var trimmed = queryText.AsSpan().TrimStart();
            if (trimmed.StartsWith("declare function", System.StringComparison.OrdinalIgnoreCase) == false)
                return false;

            int openBraces = 0;
            int closeBraces = 0;
            for (int i = 0; i < trimmed.Length; i++)
            {
                var ch = trimmed[i];

                // Single-quoted string: '' is an escaped quote.
                if (ch == '\'')
                {
                    i++;
                    while (i < trimmed.Length)
                    {
                        if (trimmed[i] == '\'')
                        {
                            if (i + 1 < trimmed.Length && trimmed[i + 1] == '\'')
                            {
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                // Double-quoted string / identifier. JS bodies use "..." for strings; skipping
                // its contents stops a stray `}` inside a JS string from balancing the body.
                if (ch == '"')
                {
                    i++;
                    while (i < trimmed.Length)
                    {
                        if (trimmed[i] == '"')
                        {
                            if (i + 1 < trimmed.Length && trimmed[i + 1] == '"')
                            {
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                if (ch == '{') openBraces++;
                else if (ch == '}') closeBraces++;
            }
            return openBraces > closeBraces;
        }

        // True if the outer SelectStmt uses INTERSECT or EXCEPT. Doesn't descend into FROM subselects;
        // only the outer combination matters here.
        private static bool HasIntersectOrExcept(SelectStmt selectStmt)
        {
            if (selectStmt == null)
                return false;
            return selectStmt.Op == SetOperation.SetopIntersect
                || selectStmt.Op == SetOperation.SetopExcept;
        }

        // True if any SelectStmt in the tree has a JoinExpr in FROM. Descends into RangeSubselect.Subquery
        // (PowerBI wraps user SQL as `select * from (...) "_"`, burying the JOIN a level deep) and set-op
        // Larg/Rarg; recursion is depth-bounded.
        private static bool HasJoinExpr(SelectStmt selectStmt) => HasJoinExpr(selectStmt, depth: 0);

        private const int MaxJoinSearchDepth = 32;

        private static bool HasJoinExpr(SelectStmt selectStmt, int depth)
        {
            if (selectStmt == null || depth >= MaxJoinSearchDepth)
                return false;

            // Recurse into UNION/INTERSECT/EXCEPT arms.
            if (selectStmt.Op != SetOperation.SetopNone)
            {
                if (HasJoinExpr(selectStmt.Larg, depth + 1)) return true;
                if (HasJoinExpr(selectStmt.Rarg, depth + 1)) return true;
            }

            if (selectStmt.FromClause == null)
                return false;

            foreach (var item in selectStmt.FromClause)
            {
                if (item == null)
                    continue;
                if (item.JoinExpr != null)
                    return true;
                if (item.RangeSubselect?.Subquery?.SelectStmt is { } inner
                    && HasJoinExpr(inner, depth + 1))
                    return true;
            }
            return false;
        }

        // True when every base relation reachable in the query's FROM / JOIN tree resolves to a
        // known virtual-catalog table (pg_catalog / information_schema). Any unresolved relation -
        // i.e. a real RavenDB collection - makes this false, preserving the RavenDB-collections
        // message for genuine user JOINs. Depth-bounded and descends into sub-SELECTs, mirroring
        // HasJoinExpr's traversal.
        private static bool AllJoinRelationsAreCatalogTables(SelectStmt selectStmt)
        {
            var relations = new List<RangeVar>();
            CollectRangeVars(selectStmt, relations, depth: 0);
            if (relations.Count == 0)
                return false;

            foreach (var rv in relations)
            {
                if (PgVirtualDatabase.TryGetTable(rv.Schemaname, rv.Relname, out _) == false)
                    return false;
            }
            return true;
        }

        private static void CollectRangeVars(SelectStmt selectStmt, List<RangeVar> acc, int depth)
        {
            if (selectStmt == null || depth >= MaxJoinSearchDepth)
                return;

            if (selectStmt.Op != SetOperation.SetopNone)
            {
                CollectRangeVars(selectStmt.Larg, acc, depth + 1);
                CollectRangeVars(selectStmt.Rarg, acc, depth + 1);
            }

            if (selectStmt.FromClause == null)
                return;

            foreach (var item in selectStmt.FromClause)
                CollectRangeVarsFromNode(item, acc, depth);
        }

        private static void CollectRangeVarsFromNode(Node node, List<RangeVar> acc, int depth)
        {
            if (node == null || depth >= MaxJoinSearchDepth)
                return;

            if (node.RangeVar != null)
                acc.Add(node.RangeVar);

            if (node.JoinExpr != null)
            {
                CollectRangeVarsFromNode(node.JoinExpr.Larg, acc, depth + 1);
                CollectRangeVarsFromNode(node.JoinExpr.Rarg, acc, depth + 1);
            }

            if (node.RangeSubselect?.Subquery?.SelectStmt is { } inner)
                CollectRangeVars(inner, acc, depth + 1);
        }

        // True iff any projection is a min()/max() FuncCall. Surfaced before the generic scalar-aggregate
        // check: min/max are unsupported with or without GROUP BY (RavenDB aggregates are Count/Sum only),
        // and their workaround (ORDER BY + LIMIT 1) differs from the "wrap in GROUP BY" hint for sum/count.
        private static bool HasMinOrMaxAggregate(SelectStmt selectStmt)
        {
            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            foreach (var t in targets)
            {
                var funcCall = t?.ResTarget?.Val?.FuncCall;
                if (funcCall == null)
                    continue;

                var name = funcCall.Funcname is { Count: > 0 }
                    ? funcCall.Funcname[funcCall.Funcname.Count - 1]?.String?.Sval
                    : null;
                if (string.IsNullOrEmpty(name))
                    continue;

                if (string.Equals(name, "min", System.StringComparison.OrdinalIgnoreCase)
                    || string.Equals(name, "max", System.StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        // True iff any projection is an avg() FuncCall. avg has no RQL form (aggregates are Count/Sum
        // only), unsupported with or without GROUP BY; surfaced before the generic scalar check so the
        // message names avg and gives the sum/count workaround.
        private static bool HasAvgAggregate(SelectStmt selectStmt)
        {
            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            foreach (var t in targets)
            {
                var funcCall = t?.ResTarget?.Val?.FuncCall;
                if (funcCall == null)
                    continue;

                var name = funcCall.Funcname is { Count: > 0 }
                    ? funcCall.Funcname[funcCall.Funcname.Count - 1]?.String?.Sval
                    : null;
                if (string.IsNullOrEmpty(name))
                    continue;

                if (string.Equals(name, "avg", System.StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        private static bool TryGetUnsupportedAggregateModifier(SelectStmt selectStmt, out string modifier)
        {
            modifier = null;

            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            foreach (var t in targets)
            {
                var funcCall = t?.ResTarget?.Val?.FuncCall;
                if (funcCall == null)
                    continue;

                if (funcCall.AggDistinct)
                    modifier = "DISTINCT";
                else if (funcCall.AggFilter != null)
                    modifier = "a FILTER (WHERE ...) clause";
                else if (funcCall.Over != null)
                    modifier = "an OVER (...) window clause";
                else
                    continue;

                return true;
            }

            return false;
        }

        private static bool IsSupportedScalarCountStar(SelectStmt selectStmt)
        {
            if (selectStmt.GroupClause is { Count: > 0 })
                return false;

            if (selectStmt.TargetList is not { Count: 1 } targets)
                return false;

            var funcCall = targets[0]?.ResTarget?.Val?.FuncCall;
            if (funcCall is not { AggStar: true })
                return false;

            if (funcCall.AggDistinct || funcCall.AggFilter != null || funcCall.Over != null)
                return false;

            var name = funcCall.Funcname is { Count: > 0 }
                ? funcCall.Funcname[funcCall.Funcname.Count - 1]?.String?.Sval
                : null;

            return string.Equals(name, "count", StringComparison.OrdinalIgnoreCase);
        }

        // Uses the translator's own WHERE parser as the oracle so the diagnoser can't drift from what is
        // actually supported, then names the offending operator when the AST identifies one.
        private static bool TryDiagnoseWhereClause(SelectStmt selectStmt, out string message)
        {
            message = null;

            if (selectStmt.WhereClause == null)
                return false;

            if (SqlWhereParser.TryParse(selectStmt.WhereClause, outerAliasToStrip: null, out _))
                return false;

            message = TryGetUnsupportedPredicateName(selectStmt.WhereClause, depth: 0, out var predicate)
                ? $"The WHERE clause uses {predicate}, which is not supported. The aggregate itself is fine — `count(*)` translates on its own — so rewrite just the predicate (for example express NOT BETWEEN as `< lower OR > upper`), or run the query as RQL."
                : "The WHERE clause could not be translated. The aggregate itself is fine — `count(*)` translates on its own — so the unsupported part is the predicate: simplify it to comparisons, IN, BETWEEN, LIKE and IS [NOT] NULL combined with AND / OR / NOT, or run the query as RQL.";

            return true;
        }

        private static bool TryGetUnsupportedPredicateName(Node node, int depth, out string predicate)
        {
            predicate = null;
            if (node == null || depth >= MaxJoinSearchDepth)
                return false;

            if (node.BoolExpr?.Args is { } args)
            {
                foreach (var arg in args)
                {
                    if (TryGetUnsupportedPredicateName(arg, depth + 1, out predicate))
                        return true;
                }

                return false;
            }

            if (node.AExpr is not { } aExpr)
                return false;

            predicate = aExpr.Kind switch
            {
                A_Expr_Kind.AexprNotBetween => "NOT BETWEEN",
                A_Expr_Kind.AexprBetweenSym => "BETWEEN SYMMETRIC",
                A_Expr_Kind.AexprNotBetweenSym => "NOT BETWEEN SYMMETRIC",
                A_Expr_Kind.AexprSimilar => "SIMILAR TO",
                A_Expr_Kind.AexprOpAny => "an ANY (...) comparison",
                A_Expr_Kind.AexprOpAll => "an ALL (...) comparison",
                _ => null
            };

            if (predicate != null)
                return true;

            return TryGetUnsupportedPredicateName(aExpr.Lexpr, depth + 1, out predicate)
                || TryGetUnsupportedPredicateName(aExpr.Rexpr, depth + 1, out predicate);
        }

        private static bool TryGetUnsupportedSortModifier(SelectStmt selectStmt, out string modifier)
        {
            modifier = null;

            if (selectStmt.SortClause is not { Count: > 0 } sortClause)
                return false;

            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode?.SortBy;
                if (sortBy == null)
                    continue;

                if (sortBy.SortbyNulls is SortByNulls.First or SortByNulls.Last)
                    modifier = "NULLS FIRST / NULLS LAST";
                else if (sortBy.SortbyDir == SortByDir.SortbyUsing)
                    modifier = "USING <operator>";
                else
                    continue;

                return true;
            }

            return false;
        }

        private static bool HasCountOverColumn(SelectStmt selectStmt)
        {
            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            foreach (var t in targets)
            {
                var funcCall = t?.ResTarget?.Val?.FuncCall;
                if (funcCall == null || funcCall.AggStar)
                    continue;

                var name = funcCall.Funcname is { Count: > 0 }
                    ? funcCall.Funcname[funcCall.Funcname.Count - 1]?.String?.Sval
                    : null;
                if (string.Equals(name, "count", System.StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (funcCall.Args is { Count: 1 } && funcCall.Args[0]?.ColumnRef != null)
                    return true;
            }

            return false;
        }

        // PG parses IS [NOT] DISTINCT FROM and NULLIF into an A_Expr whose operator name is "=", so
        // only the Kind separates them from real equality.
        private static bool HasDistinctFromPredicate(SelectStmt selectStmt)
        {
            var wheres = new List<Node>();
            CollectWhereClauses(selectStmt, wheres, depth: 0);

            foreach (var where in wheres)
            {
                if (HasDistinctFromPredicate(where, depth: 0))
                    return true;
            }

            return false;
        }

        private static bool HasDistinctFromPredicate(Node node, int depth)
        {
            if (node == null || depth >= MaxJoinSearchDepth)
                return false;

            if (node.BoolExpr?.Args is { } args)
            {
                foreach (var arg in args)
                {
                    if (HasDistinctFromPredicate(arg, depth + 1))
                        return true;
                }
                return false;
            }

            if (node.AExpr is not { } aExpr)
                return false;

            if (aExpr.Kind is A_Expr_Kind.AexprDistinct or A_Expr_Kind.AexprNotDistinct or A_Expr_Kind.AexprNullif)
                return true;

            return HasDistinctFromPredicate(aExpr.Lexpr, depth + 1)
                || HasDistinctFromPredicate(aExpr.Rexpr, depth + 1);
        }

        // True iff every projection is an aggregate FuncCall (count/sum/avg/min/max) with no GROUP BY key.
        // Mixed shapes (aggregate + bare column) aren't classified here; that's a SQL error the translator
        // already rejects with a clearer message.
        private static bool IsScalarAggregateWithoutGroupBy(SelectStmt selectStmt)
        {
            if (selectStmt.GroupClause is { Count: > 0 })
                return false;
            if (selectStmt.TargetList is not { Count: > 0 } targets)
                return false;

            foreach (var t in targets)
            {
                var funcCall = t?.ResTarget?.Val?.FuncCall;
                if (funcCall == null)
                    return false;

                var name = funcCall.Funcname is { Count: > 0 }
                    ? funcCall.Funcname[funcCall.Funcname.Count - 1]?.String?.Sval
                    : null;
                if (string.IsNullOrEmpty(name))
                    return false;

                if (IsAggregateFunctionName(name) == false)
                    return false;
            }
            return true;
        }

        private static bool IsAggregateFunctionName(string name)
        {
            // Match the SQL standard aggregate set; covers what PowerBI / pgAdmin actually emit.
            return string.Equals(name, "count", System.StringComparison.OrdinalIgnoreCase)
                || string.Equals(name, "sum",   System.StringComparison.OrdinalIgnoreCase)
                || string.Equals(name, "avg",   System.StringComparison.OrdinalIgnoreCase)
                || string.Equals(name, "min",   System.StringComparison.OrdinalIgnoreCase)
                || string.Equals(name, "max",   System.StringComparison.OrdinalIgnoreCase);
        }
    }
}
