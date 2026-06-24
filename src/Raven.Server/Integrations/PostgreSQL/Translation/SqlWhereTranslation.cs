using System;
using System.Collections.Generic;
using System.Globalization;
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
    internal sealed record ParsedIn(IReadOnlyList<string> FieldPath, IReadOnlyList<ParsedValue> Values, bool Negated) : ParsedWhere;
    internal sealed record ParsedBetween(IReadOnlyList<string> FieldPath, ParsedValue Lower, ParsedValue Upper) : ParsedWhere;
    internal sealed record ParsedIsNull(IReadOnlyList<string> FieldPath, bool Negated) : ParsedWhere;

    internal enum ParsedValueKind { String, Long, Double, Bool, Null, Timestamp, Parameter }

    // For the Parameter kind, Raw holds the 1-based PG parameter index (int) from a $N
    // placeholder — not a literal value. The value itself isn't known until Bind time.
    internal sealed record ParsedValue(object Raw, ParsedValueKind Kind);

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
    }
}
