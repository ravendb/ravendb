using System;
using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL
{
    // Splits a SQL string into statements on `;`, ignoring semicolons inside string literals,
    // quoted identifiers, comments, dollar-quoted strings, and `(...)`/`{...}`. Used by the Simple
    // Query Protocol handler to dispatch multi-statement batches (e.g. pgAdmin's startup probe)
    // one statement at a time.
    internal static class SqlStatementSplitter
    {
        public static List<string> Split(string sql)
        {
            var result = new List<string>();
            if (string.IsNullOrWhiteSpace(sql))
                return result;

            // RQL is always a single statement, but its `declare function { ... }` JS bodies
            // routinely contain `;` that a SQL splitter would shred. Bypass the splitter for
            // RQL-leading input.
            if (LooksLikeRql(sql))
            {
                result.Add(sql.Trim());
                return result;
            }

            int start = 0;
            int i = 0;
            // Track `(...)` and `{...}` depth so a `;` inside a subquery, function-arg list, or
            // RQL `declare function { ... }` JS body isn't treated as a statement boundary.
            int parenDepth = 0;
            int braceDepth = 0;
            while (i < sql.Length)
            {
                char c = sql[i];

                // Line comment: -- to end of line.
                if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\n')
                        i++;
                    continue;
                }

                // Block comment: /* ... */, PG-style nestable (/* a /* b */ c */).
                if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    i += 2;
                    int depth = 1;
                    while (i + 1 < sql.Length && depth > 0)
                    {
                        if (sql[i] == '/' && sql[i + 1] == '*')
                        {
                            depth++;
                            i += 2;
                        }
                        else if (sql[i] == '*' && sql[i + 1] == '/')
                        {
                            depth--;
                            i += 2;
                        }
                        else
                        {
                            i++;
                        }
                    }
                    // Unterminated comment: consume the rest (fail-closed).
                    if (depth > 0)
                        i = sql.Length;
                    continue;
                }

                // Single-quoted string literal: '...'. Backslash escapes are honored only in E'...' /
                // e'...' strings; with standard_conforming_strings=on (PG default) a plain '...' treats
                // \ literally, so e.g. 'C:\' keeps its closing quote. `''` is always an escaped quote.
                if (c == '\'')
                {
                    bool isEscapeString = i >= 1 && (sql[i - 1] == 'e' || sql[i - 1] == 'E')
                                          && (i < 2 || (char.IsLetterOrDigit(sql[i - 2]) == false && sql[i - 2] != '_'));
                    i++;
                    while (i < sql.Length)
                    {
                        if (isEscapeString && sql[i] == '\\' && i + 1 < sql.Length)
                        {
                            i += 2;
                            continue;
                        }
                        if (sql[i] == '\'')
                        {
                            if (i + 1 < sql.Length && sql[i + 1] == '\'')
                            {
                                i += 2;
                                continue;
                            }
                            i++;
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                // Double-quoted identifier: "...". Skip `;` inside; "" is an escaped quote.
                if (c == '"')
                {
                    i++;
                    while (i < sql.Length)
                    {
                        if (sql[i] == '"')
                        {
                            if (i + 1 < sql.Length && sql[i + 1] == '"')
                            {
                                i += 2;
                                continue;
                            }
                            i++;
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                // Dollar-quoted string: $tag$ ... $tag$ (tag may be empty: $$...$$). The tag follows
                // identifier rules - empty or letter/underscore first; a digit-first $N (e.g. $1) is a
                // positional parameter, not a dollar-quote opener.
                if (c == '$')
                {
                    int tagEnd = i + 1;
                    bool canOpen = tagEnd < sql.Length &&
                                   (sql[tagEnd] == '$' || char.IsLetter(sql[tagEnd]) || sql[tagEnd] == '_');
                    if (canOpen)
                    {
                        while (tagEnd < sql.Length && sql[tagEnd] != '$')
                        {
                            var ch = sql[tagEnd];
                            if (char.IsLetterOrDigit(ch) || ch == '_')
                            {
                                tagEnd++;
                                continue;
                            }
                            break;
                        }
                        if (tagEnd < sql.Length && sql[tagEnd] == '$')
                        {
                            var tag = sql.Substring(i, tagEnd - i + 1);
                            int close = sql.IndexOf(tag, tagEnd + 1, StringComparison.Ordinal);
                            if (close > 0)
                            {
                                i = close + tag.Length;
                                continue;
                            }
                        }
                    }
                    i++;
                    continue;
                }

                if (c == '(')
                {
                    parenDepth++;
                    i++;
                    continue;
                }
                if (c == ')')
                {
                    if (parenDepth > 0)
                        parenDepth--;
                    i++;
                    continue;
                }
                if (c == '{')
                {
                    braceDepth++;
                    i++;
                    continue;
                }
                if (c == '}')
                {
                    if (braceDepth > 0)
                        braceDepth--;
                    i++;
                    continue;
                }

                if (c == ';')
                {
                    if (parenDepth == 0 && braceDepth == 0)
                    {
                        var piece = sql.Substring(start, i - start).Trim();
                        if (piece.Length > 0)
                            result.Add(piece);
                        i++;
                        start = i;
                        continue;
                    }
                    // Nested `;` (inside `(...)` or `{...}`): not a statement boundary.
                    i++;
                    continue;
                }

                i++;
            }

            var last = sql.Substring(start, sql.Length - start).Trim();
            if (last.Length > 0)
                result.Add(last);

            return result;
        }

        // True if the first non-trivial token (after whitespace/comments) is an RQL-only leading
        // keyword (`declare` or `from`). SQL statements never start with these, so it reliably
        // marks the whole input as RQL that must not be `;`-split.
        private static bool LooksLikeRql(string sql)
        {
            int i = 0;
            while (i < sql.Length)
            {
                var ch = sql[i];
                if (char.IsWhiteSpace(ch))
                {
                    i++;
                    continue;
                }
                if (ch == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\n')
                        i++;
                    continue;
                }
                if (ch == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/'))
                        i++;
                    i = Math.Min(i + 2, sql.Length);
                    continue;
                }
                break;
            }

            return StartsWithKeywordAtWordBoundary(sql, i, "declare")
                || StartsWithKeywordAtWordBoundary(sql, i, "from");
        }

        private static bool StartsWithKeywordAtWordBoundary(string sql, int i, string keyword)
        {
            if (i + keyword.Length > sql.Length)
                return false;
            for (int k = 0; k < keyword.Length; k++)
            {
                if (char.ToLowerInvariant(sql[i + k]) != keyword[k])
                    return false;
            }
            int next = i + keyword.Length;
            if (next == sql.Length)
                return true;
            var nc = sql[next];
            return char.IsLetterOrDigit(nc) == false && nc != '_';
        }
    }
}
