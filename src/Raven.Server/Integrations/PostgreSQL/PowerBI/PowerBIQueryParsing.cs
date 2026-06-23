using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using PgSqlParser;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using RavenQuery = Raven.Server.Documents.Queries.AST.Query;
using Raven.Server.Integrations.PostgreSQL.Translation;
using Raven.Server.Logging;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    // SanitizedSql is the wrapper with innermost text replaced by `select 1` so pgsqlparser
    // can read the outer structure (real inner may be RQL or shapes pgsqlparser can't handle).
    internal sealed record InnerTextResult(
        string InnerText,
        string SanitizedSql,
        SelectStmt SanitizedSelectStmt,
        RavenQuery ResolvedQuery);

    internal static class PowerBIInnerRqlExtractor
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(PowerBIInnerRqlExtractor));

        // Extracts the inner textbox span from a PowerBI-wrapped SQL query and resolves it to a
        // RavenQuery. Returns null when extraction or resolution fails.
        public static InnerTextResult TryExtractAndResolve(string sql)
        {
            if (TryExtractInnerText(sql, out var innerText, out var sanitizedSql, out var sanitizedSelectStmt, out var fromTwoParsersPath) == false)
                return null;

            var resolved = TryResolveInnerTextToQuery(innerText, fromTwoParsersPath);
            if (resolved == null)
                return null;

            return new InnerTextResult(innerText, sanitizedSql, sanitizedSelectStmt, resolved);
        }

        private static bool TryExtractInnerText(string sql, out string innerText, out string sanitizedSql, out SelectStmt sanitizedSelectStmt, out bool fromTwoParsersPath)
        {
            innerText = null;
            sanitizedSql = null;
            sanitizedSelectStmt = null;
            fromTwoParsersPath = false;

            if (TryExtractInnerRqlSpanViaTwoParsers(sql, out var innerStart, out var innerEnd, out innerText))
            {
                fromTwoParsersPath = true;
                sanitizedSql = sql[..innerStart] + "select 1" + sql[innerEnd..];

                // Parse once and thread through so consumers reuse the SelectStmt.
                var sanitizedParse = SqlAstCache.GetOrParse(sanitizedSql);
                if (sanitizedParse.IsSuccess == false || sanitizedParse.Value?.Stmts is not { Count: 1 })
                    return false;
                sanitizedSelectStmt = sanitizedParse.Value.Stmts[0]?.Stmt?.SelectStmt;
                return sanitizedSelectStmt != null;
            }

            return TryExtractInnerSqlViaAst(sql, out innerText, out sanitizedSql, out sanitizedSelectStmt);
        }

        private static RavenQuery TryResolveInnerTextToQuery(string innerText, bool fromTwoParsersPath)
        {
            if (string.IsNullOrWhiteSpace(innerText))
                return null;

            if (fromTwoParsersPath)
            {
                // Two-parsers path already confirmed embedded RQL - parse directly.
                try
                {
                    return QueryMetadata.ParseQuery(innerText, QueryType.Select);
                }
                catch (Exception e)
                {
                    PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIInnerRqlExtractor)}: two-parsers path produced non-RQL inner text.", e);
                    return null;
                }
            }

            // AST fallback: the inner text is ambiguous. Try RQL first, then SQL to RQL translation.
            try
            {
                return QueryMetadata.ParseQuery(innerText, QueryType.Select);
            }
            catch (Exception e)
            {
                PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIInnerRqlExtractor)}: inner text is not RQL, will attempt SQL to RQL translation.", e);
            }

            if (PgSqlToRqlTranslator.TryParse(innerText, parameterTypes: Array.Empty<int>(), out var translatedRql) == false)
                return null;

            try
            {
                return QueryMetadata.ParseQuery(translatedRql, QueryType.Select);
            }
            catch (Exception e)
            {
                PowerBIRecognizerLog.Rejected(Logger, $"{nameof(PowerBIInnerRqlExtractor)}: SQL to RQL-translated query failed to re-parse as RQL.", e);
                return null;
            }
        }

        private static bool TryExtractInnerRqlSpanViaTwoParsers(string sql, out int innerStart, out int innerEnd, out string innerRql)
        {
            innerStart = 0;
            innerEnd = 0;
            innerRql = null;

            if (string.IsNullOrWhiteSpace(sql))
                return false;

            var parseResult = SqlAstCache.GetOrParse(sql);

            if (parseResult.IsSuccess)
                return false;

            var cursorPos1Based = parseResult.Error?.CursorPos ?? 0;
            if (cursorPos1Based <= 0)
                return false;
            var start = cursorPos1Based - 1;
            if (TryNormalizeStartIndex(sql, start, out start) == false)
                return false;

            if (StartsWithKeywordAtWordBoundary(sql, start, "from") == false)
            {
                if (TryRecoverDeclareFunctionStart(sql, start, out start) == false)
                    return false;
            }

            if (TryParseRqlPrefixAndGetEnd(sql, start, out var end) == false)
                return false;

            if (TryConsumeNextNonWsChar(sql, end, ')', out var afterCloseParen) == false)
                return false;

            if (TryConsumeQuotedAlias(sql, afterCloseParen) == false)
                return false;

            innerStart = start;
            innerEnd = end;
            innerRql = sql.Substring(innerStart, innerEnd - innerStart).Trim();
            return true;
        }

        private static bool TryRecoverDeclareFunctionStart(string sql, int cursorStart, out int recoveredStart)
        {
            recoveredStart = 0;

            if (string.IsNullOrWhiteSpace(sql))
                return false;

            if ((uint)cursorStart >= (uint)sql.Length)
                return false;

            int i = cursorStart - 1;

            while (i >= 0 && char.IsWhiteSpace(sql[i]))
                i--;

            if (i < 0)
                return false;

            var functionStart = i - "function".Length + 1;
            if (functionStart < 0)
                return false;

            if (StartsWithKeywordAtWordBoundary(sql, functionStart, "function") == false)
                return false;

            i = functionStart - 1;

            while (i >= 0 && char.IsWhiteSpace(sql[i]))
                i--;

            if (i < 0)
                return false;

            var declareStart = i - "declare".Length + 1;
            if (declareStart < 0)
                return false;

            if (StartsWithKeywordAtWordBoundary(sql, declareStart, "declare") == false)
                return false;

            recoveredStart = declareStart;
            return true;
        }

        private static bool TryExtractInnerSqlViaAst(string sql, out string innerSql, out string sanitizedSql, out SelectStmt sanitizedSelectStmt)
        {
            innerSql = null;
            sanitizedSql = null;
            sanitizedSelectStmt = null;

            if (string.IsNullOrWhiteSpace(sql))
                return false;

            // This method mutates the parse tree (replaces the inner subquery with `select 1`), so it
            // must NOT use the shared, reference-cached SqlAstCache tree - parse a private copy instead,
            // leaving the cached AST pristine for the other dispatch arms.
            var parseResult = Parser.Parse(sql);
            if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: 1 })
                return false;

            var rootSelect = parseResult.Value.Stmts[0]?.Stmt?.SelectStmt;
            if (rootSelect == null)
                return false;

            if (TryFindDeepestWrapperSubquery(rootSelect, out var wrapperSubqueryNode, out var innerSelect) == false)
                return false;

            if (TryDeparseSelect(innerSelect, out innerSql) == false)
                return false;

            wrapperSubqueryNode.Subquery = CreateSelect1Node();

            if (TryDeparseParseResult(parseResult.Value, out sanitizedSql) == false)
                return false;

            sanitizedSelectStmt = rootSelect;

            return string.IsNullOrWhiteSpace(innerSql) == false &&
                   string.IsNullOrWhiteSpace(sanitizedSql) == false;
        }

        private static bool TryFindDeepestWrapperSubquery(SelectStmt rootSelect, out RangeSubselect wrapperSubqueryNode, out SelectStmt innerSelect)
        {
            wrapperSubqueryNode = null;
            innerSelect = null;

            var current = rootSelect;
            var found = false;

            while (current != null)
            {
                if (current.FromClause is not { Count: 1 })
                    break;

                var rss = current.FromClause[0]?.RangeSubselect;
                if (rss == null)
                    break;

                if (PgSqlAstHelpers.IsPowerBiWrapperAlias(rss.Alias?.Aliasname) == false)
                    break;

                var next = rss.Subquery?.SelectStmt;
                if (next == null)
                    return false;

                wrapperSubqueryNode = rss;
                innerSelect = next;
                current = next;
                found = true;
            }

            return found;
        }

        private static bool TryDeparseSelect(SelectStmt selectStmt, out string sql)
        {
            sql = null;

            if (selectStmt == null)
                return false;

            var parseResult = new ParseResult();
            parseResult.Stmts.Add(new RawStmt
            {
                Stmt = new Node
                {
                    SelectStmt = selectStmt
                }
            });

            return TryDeparseParseResult(parseResult, out sql);
        }

        private static bool TryDeparseParseResult(ParseResult parseResult, out string sql)
        {
            sql = null;

            if (parseResult == null)
                return false;

            var deparseResult = Parser.Deparse(parseResult);
            if (deparseResult.IsSuccess == false)
                return false;

            sql = deparseResult.Value;
            return string.IsNullOrWhiteSpace(sql) == false;
        }

        private static Node CreateSelect1Node()
        {
            var selectStmt = new SelectStmt();

            selectStmt.TargetList.Add(new Node
            {
                ResTarget = new ResTarget
                {
                    Val = new Node
                    {
                        AConst = new A_Const
                        {
                            Ival = new Integer
                            {
                                Ival = 1
                            }
                        }
                    }
                }
            });

            return new Node
            {
                SelectStmt = selectStmt
            };
        }

        private static bool TryNormalizeStartIndex(string s, int start, out int normalizedStart)
        {
            normalizedStart = 0;

            if ((uint)start >= (uint)s.Length)
                return false;

            start = SkipWhitespaceForward(s, start);
            if (start >= s.Length)
                return false;

            normalizedStart = NormalizeIdentifierStart(s, start);
            return true;
        }

        private static int SkipWhitespaceForward(string s, int i)
        {
            while ((uint)i < (uint)s.Length && char.IsWhiteSpace(s[i]))
                i++;
            return i;
        }

        private static int NormalizeIdentifierStart(string s, int i)
        {
            if ((uint)i >= (uint)s.Length)
                return i;

            if (IsIdentChar(s[i]) == false)
                return i;

            while (i > 0 && IsIdentChar(s[i - 1]))
                i--;

            return i;
        }

        private static bool StartsWithKeywordAtWordBoundary(string s, int i, string keyword)
        {
            if ((uint)i >= (uint)s.Length)
                return false;

            if (i > 0 && IsIdentChar(s[i - 1]))
                return false;

            if (s.AsSpan(i).StartsWith(keyword, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            var end = i + keyword.Length;
            if (end < s.Length && IsIdentChar(s[end]))
                return false;

            return true;
        }

        private static bool TryParseRqlPrefixAndGetEnd(string sql, int start, out int end)
        {
            end = 0;

            var slice = sql.Substring(start);
            var qp = new QueryParser();
            qp.Init(slice);

            if (qp.TryParse(out _, out _, QueryType.Select, recursive: true) == false)
                return false;

            var consumed = qp.Scanner.Position;
            if (consumed <= 0 || consumed > slice.Length)
                return false;

            end = start + consumed;
            return end > start && end <= sql.Length;
        }

        private static bool TryConsumeNextNonWsChar(string s, int i, char expected, out int nextIndex)
        {
            nextIndex = 0;

            var idx = SkipWhitespaceForward(s, i);
            if ((uint)idx >= (uint)s.Length)
                return false;

            if (s[idx] != expected)
                return false;

            nextIndex = idx + 1;
            return true;
        }

        private static bool TryConsumeQuotedAlias(string s, int i)
        {
            var idx = SkipWhitespaceForward(s, i);
            if ((uint)idx >= (uint)s.Length || s[idx] != '"')
                return false;

            var endQuote = s.IndexOf('"', idx + 1);
            if (endQuote == -1)
                return false;

            // Reject empty/whitespace aliases like `""` or `"  "`.
            for (int j = idx + 1; j < endQuote; j++)
            {
                if (char.IsWhiteSpace(s[j]) == false)
                    return true;
            }

            return false;
        }

        private static bool IsIdentChar(char ch) =>
            (ch is >= 'A' and <= 'Z') ||
            (ch is >= 'a' and <= 'z') ||
            (ch is >= '0' and <= '9') ||
            ch == '_';

    }

    internal static class PowerBIOuterWhereTranslator
    {
        public static bool TryTranslateWhere(Node node, string outerAlias, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            if (SqlWhereParser.TryParse(node, outerAlias, out var parsed) == false)
                return false;

            return TryEmit(parsed, innerAlias, out expr);
        }

        private static bool TryEmit(ParsedWhere parsed, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            switch (parsed)
            {
                case ParsedAnd a:
                    return TryEmitBoolean(a.Children, OperatorType.And, innerAlias, out expr);

                case ParsedOr o:
                    return TryEmitBoolean(o.Children, OperatorType.Or, innerAlias, out expr);

                case ParsedNot n:
                    if (TryEmit(n.Child, innerAlias, out var inner) == false)
                        return false;
                    if (TryNegate(inner, out expr))
                        return true;
                    expr = new NegatedExpression(inner);
                    return true;

                case ParsedBinary b:
                    return TryEmitBinary(b, innerAlias, out expr);

                case ParsedIn i:
                    return TryEmitIn(i, innerAlias, out expr);

                case ParsedBetween bt:
                    return TryEmitBetween(bt, innerAlias, out expr);

                case ParsedIsNull nt:
                    return TryEmitIsNull(nt, innerAlias, out expr);

                default:
                    return false;
            }
        }

        private static bool TryEmitBoolean(IReadOnlyList<ParsedWhere> children, OperatorType op, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;
            QueryExpression current = null;
            foreach (var child in children)
            {
                if (TryEmit(child, innerAlias, out var translated) == false)
                    return false;
                current = current == null ? translated : new BinaryExpression(current, translated, op);
            }
            expr = current;
            return expr != null;
        }

        private static bool TryEmitBinary(ParsedBinary b, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            if (TryMapBinaryOperator(b.Operator, out var ravenOp) == false)
                return false;

            if (TryBuildFieldExpression(b.FieldPath, innerAlias, out var field) == false)
                return false;

            var value = BuildValueExpression(b.Value);
            if (value == null)
                return false;

            expr = new BinaryExpression(field, value, ravenOp);
            return true;
        }

        private static bool TryEmitIn(ParsedIn i, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            if (TryBuildFieldExpression(i.FieldPath, innerAlias, out var field) == false)
                return false;

            var values = new List<QueryExpression>(i.Values.Count);
            foreach (var v in i.Values)
            {
                var ve = BuildValueExpression(v);
                if (ve == null)
                    return false;
                values.Add(ve);
            }

            QueryExpression inExpr = new InExpression(field, values, all: false);
            if (i.Negated)
                inExpr = new NegatedExpression(inExpr);

            expr = inExpr;
            return true;
        }

        private static bool TryEmitBetween(ParsedBetween bt, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            if (TryBuildFieldExpression(bt.FieldPath, innerAlias, out var field) == false)
                return false;

            var lower = BuildValueExpression(bt.Lower);
            var upper = BuildValueExpression(bt.Upper);
            if (lower == null || upper == null)
                return false;

            var ge = new BinaryExpression(field, lower, OperatorType.GreaterThanEqual);
            var le = new BinaryExpression(field, upper, OperatorType.LessThanEqual);
            expr = new BinaryExpression(ge, le, OperatorType.And);
            return true;
        }

        private static bool TryEmitIsNull(ParsedIsNull nt, StringSegment? innerAlias, out QueryExpression expr)
        {
            expr = null;

            if (TryBuildFieldExpression(nt.FieldPath, innerAlias, out var field) == false)
                return false;

            var op = nt.Negated ? OperatorType.NotEqual : OperatorType.Equal;
            expr = new BinaryExpression(field, new ValueExpression("null", ValueTokenType.Null), op);
            return true;
        }

        private static bool TryBuildFieldExpression(IReadOnlyList<string> fieldPath, StringSegment? innerAlias, out FieldExpression expr)
        {
            expr = null;
            if (fieldPath == null || fieldPath.Count != 1)
                return false;

            var column = fieldPath[0];
            if (string.IsNullOrWhiteSpace(column))
                return false;

            var segments = innerAlias != null
                ? new List<StringSegment> { innerAlias.Value, column }
                : new List<StringSegment> { column };

            expr = new FieldExpression(segments);
            return true;
        }

        private static ValueExpression BuildValueExpression(ParsedValue value)
        {
            if (value == null)
                return null;

            switch (value.Kind)
            {
                case ParsedValueKind.String:
                    return new ValueExpression((string)value.Raw, ValueTokenType.String);

                case ParsedValueKind.Long:
                    return new ValueExpression(Convert.ToString((long)value.Raw, CultureInfo.InvariantCulture), ValueTokenType.Long);

                case ParsedValueKind.Double:
                    if (value.Raw is double d)
                        return new ValueExpression(d.ToString("R", CultureInfo.InvariantCulture), ValueTokenType.Double);
                    return new ValueExpression(value.Raw == null ? null : Convert.ToString(value.Raw, CultureInfo.InvariantCulture), ValueTokenType.Double);

                case ParsedValueKind.Bool:
                    return (bool)value.Raw
                        ? new ValueExpression("true", ValueTokenType.True)
                        : new ValueExpression("false", ValueTokenType.False);

                case ParsedValueKind.Timestamp:
                    return new ValueExpression((string)value.Raw, ValueTokenType.String);

                case ParsedValueKind.Null:
                    return new ValueExpression("null", ValueTokenType.Null);

                case ParsedValueKind.Parameter:
                    // $N placeholder -> RQL parameter reference. StringQueryVisitor renders a
                    // Parameter ValueExpression as "$" + token, and PgQuery.Bind keys the
                    // Parameters dict by the same 1-based PG index, so the value binds at execute.
                    return new ValueExpression(((int)value.Raw).ToString(CultureInfo.InvariantCulture), ValueTokenType.Parameter);

                default:
                    return null;
            }
        }

        private static bool TryMapBinaryOperator(string op, out OperatorType ravenOp)
        {
            ravenOp = default;
            switch (op)
            {
                case "=":  ravenOp = OperatorType.Equal;            return true;
                case "!=":
                case "<>": ravenOp = OperatorType.NotEqual;         return true;
                case "<":  ravenOp = OperatorType.LessThan;         return true;
                case "<=": ravenOp = OperatorType.LessThanEqual;    return true;
                case ">":  ravenOp = OperatorType.GreaterThan;      return true;
                case ">=": ravenOp = OperatorType.GreaterThanEqual; return true;
                default:                                            return false;
            }
        }

        private static bool TryNegate(QueryExpression expr, out QueryExpression negated)
        {
            negated = null;

            if (expr is not BinaryExpression be)
                return false;

            OperatorType inverted;
            switch (be.Operator)
            {
                case OperatorType.Equal:            inverted = OperatorType.NotEqual;         break;
                case OperatorType.NotEqual:         inverted = OperatorType.Equal;            break;
                case OperatorType.LessThan:         inverted = OperatorType.GreaterThanEqual; break;
                case OperatorType.LessThanEqual:    inverted = OperatorType.GreaterThan;      break;
                case OperatorType.GreaterThan:      inverted = OperatorType.LessThanEqual;    break;
                case OperatorType.GreaterThanEqual: inverted = OperatorType.LessThan;         break;
                default:                            return false;
            }

            negated = new BinaryExpression(be.Left, be.Right, inverted);
            return true;
        }
    }
}
