using System;
using PgSqlParser;

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
                message = "SQL JOIN over RavenDB collections is not supported. RavenDB models cross-document relationships via document IDs rather than relational joins — express the relationship in RQL using `load` / `include`, or denormalize the data into the parent document.";
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

            if (IsScalarAggregateWithoutGroupBy(outer))
            {
                message = "Scalar aggregate (e.g. `SELECT sum(...) FROM t` with no GROUP BY) is not yet supported. Wrap the query in a GROUP BY (even a constant key) or compute the aggregate client-side from the underlying rows.";
                return true;
            }

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
