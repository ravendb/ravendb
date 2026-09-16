using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using PgSqlParser;
using Sparrow.Extensions;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    // Shared WHERE IR: SqlWhereParser produces it; PgSqlToRqlTranslator and PowerBIOuterWhereTranslator consume it.
    internal abstract record ParsedWhere;

    internal sealed record ParsedAnd(IReadOnlyList<ParsedWhere> Children) : ParsedWhere;
    internal sealed record ParsedOr(IReadOnlyList<ParsedWhere> Children) : ParsedWhere;
    internal sealed record ParsedNot(ParsedWhere Child) : ParsedWhere;

    internal sealed record ParsedBinary(IReadOnlyList<string> FieldPath, string Operator, ParsedValue Value) : ParsedWhere;
    internal sealed record ParsedLike(IReadOnlyList<string> FieldPath, ParsedValue Pattern, bool CaseInsensitive, bool Negated) : ParsedWhere;
    internal sealed record ParsedIn(IReadOnlyList<string> FieldPath, IReadOnlyList<ParsedValue> Values, bool Negated) : ParsedWhere;
    internal sealed record ParsedBetween(IReadOnlyList<string> FieldPath, ParsedValue Lower, ParsedValue Upper) : ParsedWhere;
    internal sealed record ParsedIsNull(IReadOnlyList<string> FieldPath, bool Negated) : ParsedWhere;

    internal enum ParsedValueKind { String, Long, Double, Bool, Null, Timestamp, Parameter }

    // For the Parameter kind, Raw holds the 1-based PG parameter index (int) from a $N
    // placeholder — not a literal value. The value itself isn't known until Bind time.
    internal sealed record ParsedValue(object Raw, ParsedValueKind Kind);

    internal enum SqlLikeShape { Equals, StartsWith, EndsWith }

    // LIKE / ILIKE pattern shapes that map onto an RQL predicate, and readable limitations for the ones
    // that don't. Shared by the translator (which emits the RQL) and UnhandledQueryDiagnoser (which turns
    // a rejection into the wire error), so both agree on exactly which shapes are supported.
    internal static class SqlLikePattern
    {
        public static bool IsLikeOperator(string op) => op is "~~" or "!~~" or "~~*" or "!~~*";

        public static bool IsCaseInsensitive(string op) => op is "~~*" or "!~~*";

        public static bool IsNegated(string op) => op is "!~~" or "!~~*";

        public static bool TryClassify(string pattern, out SqlLikeShape shape, out string literal, out string limitation)
        {
            shape = default;
            literal = null;
            limitation = null;

            if (pattern == null)
            {
                limitation = NonLiteralPattern;
                return false;
            }

            if (pattern.Contains('\\'))
            {
                limitation = "A LIKE / ILIKE pattern containing a backslash escape (e.g. `\\%`, `\\_`) is not supported by the PostgreSQL bridge. RQL's startsWith / endsWith take a plain prefix or suffix and cannot express an escaped wildcard, so the pattern is rejected rather than matched incorrectly. Rewrite the filter without escapes, or run it as RQL.";
                return false;
            }

            if (pattern.Contains('_'))
            {
                limitation = "The LIKE / ILIKE single-character wildcard `_` is not supported by the PostgreSQL bridge. RQL has no single-character wildcard — startsWith / endsWith match a literal prefix or suffix — so the pattern is rejected rather than matched incorrectly. Use a `%` prefix / suffix pattern instead, or run the filter as RQL.";
                return false;
            }

            var wildcards = 0;
            foreach (var ch in pattern)
            {
                if (ch == '%')
                    wildcards++;
            }

            if (wildcards == 0)
            {
                shape = SqlLikeShape.Equals;
                literal = pattern;
                return true;
            }

            if (wildcards == 1)
            {
                if (pattern[^1] == '%')
                {
                    shape = SqlLikeShape.StartsWith;
                    literal = pattern[..^1];
                }
                else if (pattern[0] == '%')
                {
                    shape = SqlLikeShape.EndsWith;
                    literal = pattern[1..];
                }
                else
                {
                    limitation = InteriorWildcard;
                    return false;
                }

                if (literal.Length == 0)
                {
                    limitation = MatchesEverything;
                    return false;
                }

                return true;
            }

            if (wildcards == 2 && pattern[0] == '%' && pattern[^1] == '%' && pattern.AsSpan(1, pattern.Length - 2).Contains('%') == false)
            {
                limitation = pattern.Length == 2
                    ? MatchesEverything
                    : "A `%text%` (contains) LIKE / ILIKE pattern is not supported by the PostgreSQL bridge. RQL has no substring predicate: search() matches whole analyzed tokens rather than substrings (`search(Name, 'choc')` does not match `Chocolade`), and regex() runs against the index terms, so neither reproduces LIKE's semantics. Use a `text%` (starts-with) or `%text` (ends-with) filter instead, or run the query as RQL.";
                return false;
            }

            limitation = InteriorWildcard;
            return false;
        }

        public const string NonLiteralPattern =
            "A LIKE / ILIKE pattern that is not a plain string literal is not supported by the PostgreSQL bridge. The pattern shape decides which RQL predicate the filter becomes, so it has to be known when the statement is translated — a `$n` placeholder or an ESCAPE clause is not. Inline the pattern into the SQL text, or run the query as RQL.";

        private const string InteriorWildcard =
            "A LIKE / ILIKE pattern with a `%` wildcard in the middle (e.g. `a%b`) is not supported by the PostgreSQL bridge. RQL can express a literal prefix (startsWith) or suffix (endsWith), not an arbitrary wildcard position, so the pattern is rejected rather than matched incorrectly. Split the filter, or run the query as RQL.";

        private const string MatchesEverything =
            "A LIKE / ILIKE pattern consisting only of `%` wildcards matches every value and has no RQL equivalent. Drop the filter, or use `IS NOT NULL` if the intent was to exclude missing values.";
    }

    internal static class SqlWhereParser
    {
        public static bool TryParse(Node whereNode, string outerAliasToStrip, out ParsedWhere result)
        {
            result = null;
            if (whereNode == null)
                return false;

            if (whereNode.BoolExpr != null)
                return TryParseBoolExpr(whereNode.BoolExpr, outerAliasToStrip, out result);

            if (whereNode.AExpr != null)
                return TryParseAExpr(whereNode.AExpr, outerAliasToStrip, out result);

            if (whereNode.NullTest != null)
                return TryParseNullTest(whereNode.NullTest, outerAliasToStrip, out result);

            return false;
        }

        private static bool TryParseBoolExpr(BoolExpr boolExpr, string outerAliasToStrip, out ParsedWhere result)
        {
            result = null;
            if (boolExpr?.Args == null || boolExpr.Args.Count == 0)
                return false;

            switch (boolExpr.Boolop)
            {
                case BoolExprType.AndExpr:
                case BoolExprType.OrExpr:
                {
                    var children = new List<ParsedWhere>(boolExpr.Args.Count);
                    foreach (var arg in boolExpr.Args)
                    {
                        if (TryParse(arg, outerAliasToStrip, out var child) == false)
                            return false;
                        children.Add(child);
                    }

                    result = boolExpr.Boolop == BoolExprType.AndExpr
                        ? new ParsedAnd(children)
                        : new ParsedOr(children);
                    return true;
                }

                case BoolExprType.NotExpr:
                {
                    if (boolExpr.Args.Count != 1)
                        return false;

                    if (TryParse(boolExpr.Args[0], outerAliasToStrip, out var child) == false)
                        return false;

                    result = new ParsedNot(child);
                    return true;
                }

                default:
                    return false;
            }
        }

        private static bool TryParseAExpr(A_Expr aExpr, string outerAliasToStrip, out ParsedWhere result)
        {
            result = null;

            if (IsAExprKind(aExpr, A_Expr_Kind.AexprBetween))
            {
                if (TryExtractFieldPath(aExpr.Lexpr, outerAliasToStrip, out var field) == false)
                    return false;

                var items = aExpr.Rexpr?.List?.Items;
                if (items == null || items.Count != 2)
                    return false;

                if (TryExtractScalar(items[0], out var lower) == false)
                    return false;
                if (TryExtractScalar(items[1], out var upper) == false)
                    return false;

                result = new ParsedBetween(field, lower, upper);
                return true;
            }

            if (IsAExprKind(aExpr, A_Expr_Kind.AexprIn))
            {
                if (TryExtractFieldPath(aExpr.Lexpr, outerAliasToStrip, out var field) == false)
                    return false;

                if (TryExtractScalarList(aExpr.Rexpr, out var values) == false)
                    return false;

                var negated = TryGetBinaryOp(aExpr, out var opToken) && opToken == "<>";
                result = new ParsedIn(field, values, negated);
                return true;
            }

            if (TryGetBinaryOp(aExpr, out var op) == false)
                return false;

            if (SqlLikePattern.IsLikeOperator(op))
            {
                if (TryExtractFieldPath(aExpr.Lexpr, outerAliasToStrip, out var likeField) == false)
                    return false;

                // A rexpr that isn't a scalar (an ESCAPE clause parses as pg_catalog.like_escape(...))
                // fails here; UnhandledQueryDiagnoser names the limitation for the user.
                if (TryExtractScalar(aExpr.Rexpr, out var pattern) == false)
                    return false;

                result = new ParsedLike(likeField, pattern, SqlLikePattern.IsCaseInsensitive(op), SqlLikePattern.IsNegated(op));
                return true;
            }

            // IS [NOT] DISTINCT FROM, NULLIF and the ANY/ALL forms are all built as an A_Expr whose
            // operator name is a plain "=", so without this gate they pass IsKnownBinaryOp and become
            // an ordinary equality test — the inverse of what IS DISTINCT FROM means.
            if (IsAExprKind(aExpr, A_Expr_Kind.AexprOp) == false)
                return false;

            if (IsKnownBinaryOp(op) == false)
                return false;

            if (TryExtractFieldPath(aExpr.Lexpr, outerAliasToStrip, out var leftField) == false)
                return false;

            if (TryExtractScalar(aExpr.Rexpr, out var rightValue) == false)
                return false;

            result = new ParsedBinary(leftField, op, rightValue);
            return true;
        }

        private static bool TryParseNullTest(NullTest nullTest, string outerAliasToStrip, out ParsedWhere result)
        {
            result = null;
            if (nullTest?.Arg == null)
                return false;

            if (TryExtractFieldPath(nullTest.Arg, outerAliasToStrip, out var field) == false)
                return false;

            result = nullTest.Nulltesttype switch
            {
                NullTestType.IsNull    => new ParsedIsNull(field, Negated: false),
                NullTestType.IsNotNull => new ParsedIsNull(field, Negated: true),
                _                      => null
            };
            return result != null;
        }

        private static bool TryExtractFieldPath(Node node, string outerAliasToStrip, out IReadOnlyList<string> path)
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

            if (segments.Count > 1 &&
                string.IsNullOrWhiteSpace(outerAliasToStrip) == false &&
                string.Equals(segments[0], outerAliasToStrip, StringComparison.OrdinalIgnoreCase))
            {
                segments.RemoveAt(0);
            }

            if (segments.Count == 0)
                return false;

            path = segments;
            return true;
        }

        private static bool TryExtractScalar(Node node, out ParsedValue value)
        {
            value = null;
            if (node == null)
                return false;

            // $N parameter placeholder. In the Extended Query Protocol the value isn't known at
            // translate time (Parse precedes Bind), so we can't inline a literal. Carry the
            // 1-based parameter index; the translator emits an RQL parameter reference ($N) that
            // the Bind-time values fill in. PG numbers parameters from 1 — reject anything else
            // so we never emit an unbindable $0.
            if (node.ParamRef != null)
            {
                if (node.ParamRef.Number < 1)
                    return false;
                value = new ParsedValue(node.ParamRef.Number, ParsedValueKind.Parameter);
                return true;
            }

            if (TryExtractTimestampLiteral(node, out value))
                return true;

            if (TryExtractTimestampFunction(node, out value))
                return true;

            var c = node.AConst;
            if (c == null)
                return false;

            if (c.Sval != null && c.Sval.Sval != null)
            {
                // Empty string '' is a valid SQL literal — `WHERE col = ''` is a common
                // idiom for "filter to rows where col is the empty string". Don't reject
                // c.Sval.Sval == "" here; it's distinct from c.Sval being null.
                value = new ParsedValue(c.Sval.Sval, ParsedValueKind.String);
                return true;
            }

            if (c.Ival != null)
            {
                value = new ParsedValue((long)c.Ival.Ival, ParsedValueKind.Long);
                return true;
            }

            if (c.Fval != null && string.IsNullOrEmpty(c.Fval.Fval) == false)
            {
                // pgsqlparser serialises floats as strings.
                if (double.TryParse(c.Fval.Fval, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    value = new ParsedValue(d, ParsedValueKind.Double);
                else
                    value = new ParsedValue(c.Fval.Fval, ParsedValueKind.Double);
                return true;
            }

            if (c.Boolval != null)
            {
                value = new ParsedValue(c.Boolval.Boolval, ParsedValueKind.Bool);
                return true;
            }

            return false;
        }

        private static bool TryExtractScalarList(Node node, out IReadOnlyList<ParsedValue> values)
        {
            values = null;
            var items = node?.List?.Items;
            if (items == null || items.Count == 0)
                return false;

            if (items.Count == 1 && items[0]?.List?.Items != null)
                items = items[0].List.Items;

            var result = new List<ParsedValue>(items.Count);
            foreach (var item in items)
            {
                if (TryExtractScalar(item, out var v) == false)
                    return false;
                result.Add(v);
            }

            if (result.Count == 0)
                return false;

            values = result;
            return true;
        }

        private static bool TryGetBinaryOp(A_Expr aExpr, out string op)
        {
            op = null;
            if (aExpr?.Name == null || aExpr.Name.Count != 1)
                return false;

            var s = aExpr.Name[0]?.String?.Sval;
            if (string.IsNullOrWhiteSpace(s))
                return false;

            op = s.Trim();
            return true;
        }

        private static bool IsKnownBinaryOp(string op) =>
            op is "=" or "!=" or "<>" or "<" or "<=" or ">" or ">=";

        private static bool IsAExprKind(A_Expr expr, A_Expr_Kind kind) =>
            expr?.Kind == kind;

        private static bool TryExtractTimestampLiteral(Node node, out ParsedValue value)
        {
            value = null;
            var typeCast = node?.TypeCast;
            if (typeCast == null)
                return false;

            var names = typeCast.TypeName?.Names;
            if (names == null)
                return false;

            var hasTimestamp = false;
            foreach (var nameNode in names)
            {
                if (string.Equals(nameNode?.String?.Sval, "timestamp", StringComparison.OrdinalIgnoreCase))
                {
                    hasTimestamp = true;
                    break;
                }
            }

            if (hasTimestamp == false)
                return false;

            var raw = typeCast.Arg?.AConst?.Sval?.Sval;
            if (raw == null)
                return false;

            // Parse as Unspecified (no TZ shift) to match the original PowerBI translator behaviour.
            if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt) == false)
                return false;

            value = new ParsedValue(dt.GetDefaultRavenFormat(), ParsedValueKind.Timestamp);
            return true;
        }

        // Superset's Time Range filter emits every datetime bound as to_timestamp(<literal>, <format>).
        private static bool TryExtractTimestampFunction(Node node, out ParsedValue value)
        {
            value = null;

            if (node?.FuncCall?.Funcname is not { Count: > 0 } names)
                return false;

            var funcName = names[names.Count - 1]?.String?.Sval;
            var dateOnly = string.Equals(funcName, "to_date", StringComparison.OrdinalIgnoreCase);
            if (dateOnly == false && string.Equals(funcName, "to_timestamp", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            // The single-argument to_timestamp(epoch) overload takes no format string; only the
            // two-literal form is recognised, everything else falls through to rejection.
            if (node.FuncCall.Args is not { Count: 2 } args)
                return false;

            var raw = args[0]?.AConst?.Sval?.Sval;
            var format = args[1]?.AConst?.Sval?.Sval;
            if (raw == null || format == null)
                return false;

            if (TryConvertPgDateFormat(format, out var netFormat) == false)
                return false;

            if (DateTime.TryParseExact(raw, netFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt) == false)
                return false;

            if (dateOnly)
                dt = dt.Date;

            value = new ParsedValue(dt.GetDefaultRavenFormat(), ParsedValueKind.Timestamp);
            return true;
        }

        private static readonly (string Pg, string Net)[] PgDateFormatTokens =
        {
            ("YYYY", "yyyy"),
            ("HH24", "HH"),
            ("MM", "MM"),
            ("DD", "dd"),
            ("MI", "mm"),
            ("SS", "ss"),
            ("US", "ffffff"),
            ("MS", "fff"),
        };

        private const string PgDateFormatSeparators = "-/:. ,T";

        // Only the subset of PG's format language listed above translates; an unrecognised token has to
        // fail the whole format rather than be passed through as a literal, which would shift the value.
        private static bool TryConvertPgDateFormat(string format, out string netFormat)
        {
            netFormat = null;
            if (string.IsNullOrWhiteSpace(format))
                return false;

            var builder = new StringBuilder(format.Length * 2);
            var i = 0;
            while (i < format.Length)
            {
                var matched = false;
                foreach (var (pg, net) in PgDateFormatTokens)
                {
                    if (i + pg.Length > format.Length)
                        continue;

                    if (string.Compare(format, i, pg, 0, pg.Length, StringComparison.OrdinalIgnoreCase) != 0)
                        continue;

                    builder.Append(net);
                    i += pg.Length;
                    matched = true;
                    break;
                }

                if (matched)
                    continue;

                if (PgDateFormatSeparators.IndexOf(format[i]) < 0)
                    return false;

                builder.Append('\\').Append(format[i]);
                i++;
            }

            netFormat = builder.ToString();
            return true;
        }
    }
}
