using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PgSqlParser;
using Raven.Server.Integrations.PostgreSQL.Exceptions;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog
{
    // Evaluates a value expression (parser Node) against a RowScope. Mirror of PredicateEvaluator
    // but on the raw AST - projection targets and CASE WHEN result clauses don't have a
    // ParsedWhere IR. Handles: AConst literals, ColumnRef, CaseExpr, NullTest, BoolExpr/AExpr,
    // SubLink, and a narrow FuncCall slice (the catalog scalar functions).
    internal static class ExpressionEvaluator
    {
        // A subquery resolver injected by the interpreter so this class doesn't need to depend on
        // the rest of the pipeline. Returns the list of single-column row values from the inner
        // SELECT. The outerScope argument carries the surrounding query's current row, which the
        // inner SELECT can reference via correlated column lookups (e.g.
        // `WHERE inner.id = outer.id` inside an ARRAY(...) subquery).
        //
        // The caller decides how to interpret the returned list based on the SubLinkType: scalar
        // EXPR_SUBLINK expects 0 or 1 element; ARRAY_SUBLINK takes the full list as an array value.
        public delegate bool ScalarSubqueryResolver(SelectStmt subquery, RowScope outerScope, out IReadOnlyList<object> values);

        // A scalar-function resolver injected by the interpreter - given a function name and
        // pre-evaluated args, returns the function's value. Used for inline calls like
        // `current_database()`, `pg_encoding_to_char(encoding)`, etc.
        public delegate bool ScalarFunctionResolver(string name, IReadOnlyList<object> args, out object value);

        public static bool TryEvaluate(Node node, RowScope scope, out object value)
            => TryEvaluate(node, scope, subqueryResolver: null, functionResolver: null, out value);

        public static bool TryEvaluate(Node node, RowScope scope, ScalarSubqueryResolver subqueryResolver, out object value)
            => TryEvaluate(node, scope, subqueryResolver, functionResolver: null, out value);

        public static bool TryEvaluate(Node node, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            value = null;
            if (node == null)
                return false;

            if (node.AConst != null)
                return TryEvaluateConst(node.AConst, out value);

            if (node.ColumnRef != null)
                return TryEvaluateColumnRef(node.ColumnRef, scope, out value);

            if (node.CaseExpr != null)
                return TryEvaluateCase(node.CaseExpr, scope, subqueryResolver, functionResolver, out value);

            if (node.NullTest != null)
                return TryEvaluateNullTest(node.NullTest, scope, subqueryResolver, functionResolver, out value);

            if (node.BoolExpr != null)
                return TryEvaluateBoolExpr(node.BoolExpr, scope, subqueryResolver, functionResolver, out value);

            if (node.AExpr != null)
                return TryEvaluateAExpr(node.AExpr, scope, subqueryResolver, functionResolver, out value);

            if (node.TypeCast != null)
                return TryEvaluate(node.TypeCast.Arg, scope, subqueryResolver, functionResolver, out value);

            if (node.RelabelType != null)
                return TryEvaluate(node.RelabelType.Arg, scope, subqueryResolver, functionResolver, out value);

            // SubLink: `(SELECT ...)` used as a value, or `ARRAY(SELECT ...)` building an array.
            // The interpreter passes the resolver in; we distinguish by SubLinkType. The current
            // scope is forwarded as the outer scope so the inner SELECT can correlate (e.g.
            // `WHERE inner.id = outer.id`).
            if (node.SubLink != null && subqueryResolver != null)
                return TryEvaluateSubLink(node.SubLink, scope, subqueryResolver, out value);

            // Inline scalar function call: current_database(), pg_encoding_to_char(x), etc.
            if (node.FuncCall != null && functionResolver != null)
                return TryEvaluateFuncCall(node.FuncCall, scope, subqueryResolver, functionResolver, out value);

            // SQL keyword value functions: `current_user`, `session_user`, `current_database`,
            // etc. when written without parens. PG parses these as a separate AST node, not as a
            // FuncCall - route them through the same function resolver as parenthesized forms.
            if (node.SqlvalueFunction != null && functionResolver != null)
                return TryEvaluateSqlValueFunction(node.SqlvalueFunction, functionResolver, out value);

            // ParamRef ($N): the parameter isn't bound at interpret time - the interpreter runs at
            // Parse-time (Extended Query Protocol), before the Bind step. We resolve to NULL, which
            // propagates through PG's three-valued logic: `oid = ANY($1)` becomes NULL -> row excluded.
            // The net effect is an empty rowset with the right column shape, which is the correct
            // degraded behavior for pgAdmin's type-introspection probe (it falls back to showing
            // raw oids when the typname lookup returns nothing).
            if (node.ParamRef != null)
            {
                value = null;
                return true;
            }

            return false;
        }

        private static bool TryEvaluateSqlValueFunction(SQLValueFunction svf, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;
            var name = svf.Op switch
            {
                SQLValueFunctionOp.SvfopCurrentUser    => "current_user",
                SQLValueFunctionOp.SvfopSessionUser    => "session_user",
                SQLValueFunctionOp.SvfopUser           => "user",
                SQLValueFunctionOp.SvfopCurrentCatalog => "current_database",  // PG synonym
                SQLValueFunctionOp.SvfopCurrentSchema  => "current_schema",
                SQLValueFunctionOp.SvfopCurrentRole    => "current_user",      // role == user for our purposes
                _ => null,
            };
            if (name == null)
                return false;
            return functionResolver(name, System.Array.Empty<object>(), out value);
        }

        private static bool TryEvaluateSubLink(SubLink subLink, RowScope scope, ScalarSubqueryResolver subqueryResolver, out object value)
        {
            value = null;
            var inner = subLink.Subselect?.SelectStmt;
            if (inner == null)
                return false;

            if (subqueryResolver(inner, scope, out var values) == false)
                return false;

            // ARRAY(...) - `ARRAY(SELECT col FROM ...)` yields the full list as an array value.
            // pgsqlparser models this as SubLinkType.ArraySublink.
            if (subLink.SubLinkType == SubLinkType.ArraySublink)
            {
                value = values; // IReadOnlyList<object>
                return true;
            }

            // EXISTS - pure cardinality predicate, projection values are irrelevant. pgAdmin's
            // schema-tree probe uses this to gate per-namespace WHERE clauses, e.g.
            // `EXISTS (SELECT 1 FROM pg_class WHERE relname='pg_class' AND relnamespace=nsp.oid)`.
            // The subquery has already executed (correlated outer columns resolved via `scope`);
            // we just need to know whether it produced any rows. NULL row values still count as
            // existing rows in PG's EXISTS semantics - we agree by counting unconditionally.
            if (subLink.SubLinkType == SubLinkType.ExistsSublink)
            {
                value = values.Count > 0;
                return true;
            }

            // Scalar (EXPR_SUBLINK): 0 -> null, 1 -> the value, anything else -> fail (we don't
            // model the cardinality-violation error other PG implementations throw).
            if (values.Count == 0)
            {
                value = null;
                return true;
            }
            if (values.Count > 1)
                return false;
            value = values[0];
            return true;
        }

        private static bool TryEvaluateFuncCall(FuncCall funcCall, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;

            if (funcCall.Funcname is not { Count: > 0 } parts)
                return false;
            // Multi-part names like pg_catalog.pg_encoding_to_char -> use the last segment.
            var name = parts[^1]?.String?.Sval;
            if (string.IsNullOrEmpty(name))
                return false;

            var args = new List<object>();
            if (funcCall.Args != null)
            {
                foreach (var arg in funcCall.Args)
                {
                    if (TryEvaluate(arg, scope, subqueryResolver, functionResolver, out var argValue) == false)
                        return false;
                    args.Add(argValue);
                }
            }

            return functionResolver(name, args, out value);
        }

        private static bool TryEvaluateConst(A_Const c, out object value)
        {
            value = null;

            if (c.Isnull)
                return true;

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
            if (c.Fval != null && string.IsNullOrEmpty(c.Fval.Fval) == false)
            {
                if (double.TryParse(c.Fval.Fval, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    value = d;
                else
                    value = c.Fval.Fval;
                return true;
            }
            if (c.Boolval != null)
            {
                value = c.Boolval.Boolval;
                return true;
            }

            return false;
        }

        private static bool TryEvaluateColumnRef(ColumnRef colRef, RowScope scope, out object value)
        {
            value = null;
            if (colRef.Fields is not { Count: > 0 } fields)
                return false;

            var path = new List<string>(fields.Count);
            foreach (var f in fields)
            {
                var s = f?.String?.Sval;
                if (string.IsNullOrWhiteSpace(s))
                    return false;
                path.Add(s);
            }

            return scope.TryLookup(path, out value);
        }

        private static bool TryEvaluateCase(CaseExpr caseExpr, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;
            if (caseExpr.Args is not { Count: > 0 } whens)
                return false;

            foreach (var whenNode in whens)
            {
                var when = whenNode?.CaseWhen;
                if (when == null)
                    return false;

                if (TryEvaluate(when.Expr, scope, subqueryResolver, functionResolver, out var cond) == false)
                    return false;

                if (IsTruthy(cond))
                {
                    return TryEvaluate(when.Result, scope, subqueryResolver, functionResolver, out value);
                }
            }

            if (caseExpr.Defresult != null)
                return TryEvaluate(caseExpr.Defresult, scope, subqueryResolver, functionResolver, out value);

            // No WHEN matched and no ELSE -> SQL NULL.
            value = null;
            return true;
        }

        private static bool TryEvaluateNullTest(NullTest nullTest, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;
            if (TryEvaluate(nullTest.Arg, scope, subqueryResolver, functionResolver, out var inner) == false)
                return false;

            var isNull = inner is null;
            value = nullTest.Nulltesttype switch
            {
                NullTestType.IsNull    => isNull,
                NullTestType.IsNotNull => isNull == false,
                _ => (object)null
            };
            return value != null;
        }

        private static bool TryEvaluateBoolExpr(BoolExpr boolExpr, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;
            if (boolExpr?.Args is not { Count: > 0 } args)
                return false;

            switch (boolExpr.Boolop)
            {
                case BoolExprType.AndExpr:
                    foreach (var arg in args)
                    {
                        if (TryEvaluate(arg, scope, subqueryResolver, functionResolver, out var childAnd) == false)
                            return false;
                        if (IsTruthy(childAnd) == false)
                        {
                            value = false;
                            return true;
                        }
                    }
                    value = true;
                    return true;

                case BoolExprType.OrExpr:
                    foreach (var arg in args)
                    {
                        if (TryEvaluate(arg, scope, subqueryResolver, functionResolver, out var childOr) == false)
                            return false;
                        if (IsTruthy(childOr))
                        {
                            value = true;
                            return true;
                        }
                    }
                    value = false;
                    return true;

                case BoolExprType.NotExpr:
                    if (args.Count != 1)
                        return false;
                    if (TryEvaluate(args[0], scope, subqueryResolver, functionResolver, out var inner) == false)
                        return false;
                    value = IsTruthy(inner) == false;
                    return true;

                default:
                    return false;
            }
        }

        private static bool TryEvaluateAExpr(A_Expr aExpr, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;

            if (aExpr.Kind == A_Expr_Kind.AexprIn)
                return TryEvaluateInExpr(aExpr, scope, subqueryResolver, functionResolver, out value);

            // `x op ANY(array_expr)` - true if x op element holds for any array element. The
            // canonical use is `x = ANY(ARRAY(...))`, and we only handle equality here (extend
            // when other operators show up).
            if (aExpr.Kind == A_Expr_Kind.AexprOpAny)
            {
                if (TryEvaluate(aExpr.Lexpr, scope, subqueryResolver, functionResolver, out var anyLhs) == false)
                    return false;
                if (TryEvaluate(aExpr.Rexpr, scope, subqueryResolver, functionResolver, out var anyRhs) == false)
                    return false;

                if (aExpr.Name is not { Count: 1 } || aExpr.Name[0]?.String?.Sval != "=")
                    return false; // Only `= ANY(...)` for now.

                if (anyRhs is not System.Collections.IEnumerable enumerable)
                {
                    // NULL on the array side is SQL NULL - propagate.
                    if (anyRhs is null)
                    {
                        value = null;
                        return true;
                    }
                    return false;
                }
                foreach (var item in enumerable)
                {
                    if (item == null) continue;
                    var anyCmp = CompareValues(anyLhs, item);
                    if (anyCmp == 0)
                    {
                        value = true;
                        return true;
                    }
                }
                value = false;
                return true;
            }

            if (aExpr.Name is not { Count: 1 })
                return false;

            var op = aExpr.Name[0]?.String?.Sval;
            if (string.IsNullOrEmpty(op))
                return false;

            if (TryEvaluate(aExpr.Lexpr, scope, subqueryResolver, functionResolver, out var lhs) == false)
                return false;
            if (TryEvaluate(aExpr.Rexpr, scope, subqueryResolver, functionResolver, out var rhs) == false)
                return false;

            // LIKE (~~), NOT LIKE (!~~), ILIKE (~~*), NOT ILIKE (!~~*).
            // PG's parser folds these into A_Expr with the operator carrying the matching semantics
            // (Kind is AexprLike/AexprIlike or AexprOp depending on parser version). Three-valued
            // logic: NULL on either side yields NULL.
            if (op is "~~" or "!~~" or "~~*" or "!~~*")
            {
                if (lhs is null || rhs is null)
                {
                    value = null;
                    return true;
                }
                var ignoreCase = op is "~~*" or "!~~*";
                var matched = MatchLikePattern(lhs.ToString(), rhs.ToString(), ignoreCase);
                value = op is "!~~" or "!~~*" ? !matched : matched;
                return true;
            }

            // String concatenation. PG: NULL || anything -> NULL (strict).
            if (op == "||")
            {
                if (lhs is null || rhs is null)
                {
                    value = null;
                    return true;
                }
                value = lhs.ToString() + rhs.ToString();
                return true;
            }

            var cmp = CompareValues(lhs, rhs);
            if (cmp == null)
            {
                // SQL three-valued logic: a NULL operand makes every comparison (including <> / !=)
                // yield NULL, which IsTruthy treats as not-true so the row is excluded.
                value = null;
                return true;
            }

            value = op switch
            {
                "="  => cmp.Value == 0,
                "!=" => cmp.Value != 0,
                "<>" => cmp.Value != 0,
                "<"  => cmp.Value < 0,
                "<=" => cmp.Value <= 0,
                ">"  => cmp.Value > 0,
                ">=" => cmp.Value >= 0,
                _ => (object)null
            };
            return value != null;
        }

        // LIKE pattern -> anchored regex, cached so a predicate evaluated over many catalog rows
        // compiles each distinct pattern once instead of per row. Bounded to cap growth from
        // adversarial distinct patterns; once full, misses still match correctly, just uncached.
        private static readonly ConcurrentDictionary<(string Pattern, bool IgnoreCase), Regex> LikeRegexCache = new();
        private const int LikeRegexCacheCap = 1024;
        private static readonly System.TimeSpan LikeMatchTimeout = System.TimeSpan.FromSeconds(1);

        private static bool MatchLikePattern(string input, string pattern, bool ignoreCase)
        {
            var key = (pattern, ignoreCase);
            if (LikeRegexCache.TryGetValue(key, out var regex) == false)
            {
                regex = BuildLikeRegex(pattern, ignoreCase);
                if (LikeRegexCache.Count < LikeRegexCacheCap)
                    LikeRegexCache.TryAdd(key, regex);
            }

            try
            {
                return regex.IsMatch(input);
            }
            catch (RegexMatchTimeoutException)
            {
                throw new PgErrorException(PgErrorCodes.StatementTooComplex, $"LIKE pattern took too long to evaluate: {pattern}");
            }
        }

        // Translate SQL LIKE wildcards to an anchored .NET regex.
        //   %   -> .*        (any sequence, including empty)
        //   _   -> .         (any single char)
        //   \X  -> literal X (PG default escape with standard_conforming_strings on)
        // Regex-meta chars in the pattern are escaped so they match literally.
        private static Regex BuildLikeRegex(string pattern, bool ignoreCase)
        {
            var sb = new System.Text.StringBuilder(pattern.Length + 4);
            sb.Append('^');
            for (int i = 0; i < pattern.Length; i++)
            {
                var c = pattern[i];
                switch (c)
                {
                    case '%':
                        sb.Append(".*");
                        break;
                    case '_':
                        sb.Append('.');
                        break;
                    case '\\':
                        // A trailing escape has nothing to escape - match a literal backslash
                        // rather than silently dropping it.
                        if (i + 1 < pattern.Length)
                            sb.Append(Regex.Escape(pattern[++i].ToString()));
                        else
                            sb.Append("\\\\");
                        break;
                    default:
                        if ("\\^$.|?*+()[]{}".IndexOf(c) >= 0)
                            sb.Append('\\');
                        sb.Append(c);
                        break;
                }
            }
            sb.Append('$');

            var options = RegexOptions.Singleline | RegexOptions.CultureInvariant;
            if (ignoreCase)
                options |= RegexOptions.IgnoreCase;
            return new Regex(sb.ToString(), options, LikeMatchTimeout);
        }

        private static bool TryEvaluateInExpr(A_Expr aExpr, RowScope scope, ScalarSubqueryResolver subqueryResolver, ScalarFunctionResolver functionResolver, out object value)
        {
            value = null;
            if (TryEvaluate(aExpr.Lexpr, scope, subqueryResolver, functionResolver, out var lhs) == false)
                return false;

            var items = aExpr.Rexpr?.List?.Items;
            if (items == null)
                return false;
            if (items.Count == 1 && items[0]?.List?.Items != null)
                items = items[0].List.Items;

            var negated = false;
            if (aExpr.Name is { Count: 1 } && aExpr.Name[0]?.String?.Sval == "<>")
                negated = true;

            var sawNull = false;
            foreach (var item in items)
            {
                if (TryEvaluate(item, scope, subqueryResolver, functionResolver, out var candidate) == false)
                    return false;
                var cmp = CompareValues(lhs, candidate);
                if (cmp == null)
                {
                    sawNull = true; // NULL operand -> undetermined membership (SQL 3VL)
                    continue;
                }
                if (cmp == 0)
                {
                    value = negated == false;
                    return true;
                }
            }

            // No definite match: if any comparison was NULL (lhs or a candidate), membership is SQL
            // NULL - excluded - so `x NOT IN (..., NULL)` / `NULL IN (...)` don't spuriously include
            // the row. Only a fully-determined non-match yields the boolean result.
            if (sawNull)
            {
                value = null;
                return true;
            }

            value = negated;
            return true;
        }

        // Comparison semantics used by both = / <> / < and IN. Null on either side -> null result
        // (matches SQL three-valued logic; callers fall through to the next CASE WHEN).
        private static int? CompareValues(object lhs, object rhs)
        {
            if (lhs is null || rhs is null)
                return null;

            if (lhs is bool lb && rhs is bool rb)
                return lb.CompareTo(rb);

            if (TryToLong(lhs, out var ll) && TryToLong(rhs, out var rl))
                return ll.CompareTo(rl);

            if (TryToDouble(lhs, out var ld) && TryToDouble(rhs, out var rd))
                return ld.CompareTo(rd);

            var ls = lhs.ToString();
            var rs = rhs.ToString();
            return string.CompareOrdinal(ls, rs);
        }

        private static bool TryToLong(object v, out long result)
        {
            switch (v)
            {
                case long l: result = l; return true;
                case int i: result = i; return true;
                case short s: result = s; return true;
                case byte b: result = b; return true;
                case string str when long.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p):
                    result = p; return true;
                default:
                    result = 0; return false;
            }
        }

        private static bool TryToDouble(object v, out double result)
        {
            switch (v)
            {
                case double d: result = d; return true;
                case float f: result = f; return true;
                case long l: result = l; return true;
                case int i: result = i; return true;
                case string str when double.TryParse(str, NumberStyles.Float, CultureInfo.InvariantCulture, out var p):
                    result = p; return true;
                default:
                    result = 0; return false;
            }
        }

        public static bool IsTruthy(object value)
            => value switch
            {
                null => false,
                bool b => b,
                _ => true,
            };
    }
}
