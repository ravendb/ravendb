using System;
using System.Collections.Generic;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL
{
    internal static class SelectStmtShape
    {
        public static bool TryParseSingleSelect(string queryText, out SelectStmt selectStmt)
        {
            selectStmt = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            var parseResult = SqlAstCache.GetOrParse(queryText);
            if (parseResult.IsSuccess == false || parseResult.Value?.Stmts is not { Count: 1 })
                return false;

            selectStmt = parseResult.Value.Stmts[0]?.Stmt?.SelectStmt;
            return selectStmt != null;
        }

        public static bool TryParseSelectStatements(string queryText, out IReadOnlyList<SelectStmt> selects)
        {
            selects = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            var parseResult = SqlAstCache.GetOrParse(queryText);
            if (parseResult.IsSuccess == false || parseResult.Value?.Stmts == null)
                return false;

            var stmts = parseResult.Value.Stmts;
            if (stmts.Count == 0)
                return false;

            var result = new SelectStmt[stmts.Count];
            for (int i = 0; i < stmts.Count; i++)
            {
                var s = stmts[i]?.Stmt?.SelectStmt;
                if (s == null)
                    return false;
                result[i] = s;
            }

            selects = result;
            return true;
        }

        public static bool HasNoFromClause(SelectStmt s)
            => s?.FromClause is not { Count: > 0 };

        public static bool IsSingleUnqualifiedFunctionCall(SelectStmt s, string expectedName, int expectedArgCount, out FuncCall funcCall)
        {
            funcCall = null;

            if (s?.TargetList is not { Count: 1 })
                return false;

            funcCall = s.TargetList[0]?.ResTarget?.Val?.FuncCall;
            if (funcCall == null)
                return false;

            if (funcCall.Funcname is not { Count: 1 })
                return false;

            var name = funcCall.Funcname[0].String?.Sval;
            if (string.Equals(name, expectedName, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            return (funcCall.Args?.Count ?? 0) == expectedArgCount;
        }

        public static bool HasWildcardTarget(SelectStmt s)
        {
            if (s?.TargetList == null)
                return false;

            foreach (var target in s.TargetList)
            {
                var fields = target?.ResTarget?.Val?.ColumnRef?.Fields;
                if (fields == null)
                    continue;

                foreach (var field in fields)
                {
                    if (field?.AStar != null)
                        return true;
                }
            }

            return false;
        }

        public static HashSet<string> CollectProjectedNames(SelectStmt s)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (s?.TargetList == null)
                return names;

            foreach (var target in s.TargetList)
            {
                var rt = target?.ResTarget;
                if (rt == null)
                    continue;

                if (string.IsNullOrWhiteSpace(rt.Name) == false)
                {
                    names.Add(rt.Name);
                    continue;
                }

                var fields = rt.Val?.ColumnRef?.Fields;
                if (fields is { Count: > 0 })
                {
                    var lastName = fields[^1]?.String?.Sval;
                    if (string.IsNullOrWhiteSpace(lastName) == false)
                        names.Add(lastName);
                }
            }

            return names;
        }

        public static bool ProjectedNamesContainAll(SelectStmt s, params string[] required)
        {
            var names = CollectProjectedNames(s);
            foreach (var r in required)
            {
                if (names.Contains(r) == false)
                    return false;
            }
            return true;
        }

        public static bool ProjectedNamesEqual(SelectStmt s, params string[] expected)
        {
            var names = CollectProjectedNames(s);
            if (names.Count != expected.Length)
                return false;

            foreach (var e in expected)
            {
                if (names.Contains(e) == false)
                    return false;
            }

            return true;
        }

        public static bool ProjectsName(SelectStmt s, string expectedName)
        {
            var names = CollectProjectedNames(s);
            return names.Contains(expectedName);
        }
    }
}
