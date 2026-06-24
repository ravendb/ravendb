using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class SqlStatementSplitterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Empty_input_returns_empty_list()
        {
            Assert.Empty(SqlStatementSplitter.Split(""));
            Assert.Empty(SqlStatementSplitter.Split("   \n\t "));
            Assert.Empty(SqlStatementSplitter.Split(null));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Single_statement_no_trailing_semicolon_returns_one_element()
        {
            var parts = SqlStatementSplitter.Split("SELECT 1");
            Assert.Single(parts);
            Assert.Equal("SELECT 1", parts[0]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Single_statement_trailing_semicolon_is_normalized()
        {
            var parts = SqlStatementSplitter.Split("SELECT 1;");
            Assert.Single(parts);
            Assert.Equal("SELECT 1", parts[0]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Multiple_statements_split_on_semicolons()
        {
            var parts = SqlStatementSplitter.Split("SET a=1; SET b=2; SELECT 3");
            Assert.Equal(3, parts.Count);
            Assert.Equal("SET a=1", parts[0]);
            Assert.Equal("SET b=2", parts[1]);
            Assert.Equal("SELECT 3", parts[2]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pgadmin_startup_probe_splits_into_four_pieces()
        {
            const string sql = "SET DateStyle=ISO; SET client_min_messages=notice; " +
                               "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'; " +
                               "SET client_encoding='utf-8';";
            var parts = SqlStatementSplitter.Split(sql);
            Assert.Equal(4, parts.Count);
            Assert.StartsWith("SET DateStyle", parts[0]);
            Assert.StartsWith("SET client_min_messages", parts[1]);
            Assert.StartsWith("SELECT set_config", parts[2]);
            Assert.StartsWith("SET client_encoding", parts[3]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolon_inside_single_quoted_string_is_not_a_separator()
        {
            var parts = SqlStatementSplitter.Split("SELECT 'a;b'; SELECT 'c'");
            Assert.Equal(2, parts.Count);
            Assert.Equal("SELECT 'a;b'", parts[0]);
            Assert.Equal("SELECT 'c'", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolon_inside_double_quoted_identifier_is_not_a_separator()
        {
            var parts = SqlStatementSplitter.Split("SELECT \"a;b\"; SELECT \"c\"");
            Assert.Equal(2, parts.Count);
            Assert.Equal("SELECT \"a;b\"", parts[0]);
            Assert.Equal("SELECT \"c\"", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolon_inside_line_comment_is_not_a_separator()
        {
            var parts = SqlStatementSplitter.Split("SELECT 1 -- a;b\n; SELECT 2");
            Assert.Equal(2, parts.Count);
            Assert.StartsWith("SELECT 1", parts[0]);
            Assert.Equal("SELECT 2", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolon_inside_block_comment_is_not_a_separator()
        {
            var parts = SqlStatementSplitter.Split("SELECT /* a;b */ 1; SELECT 2");
            Assert.Equal(2, parts.Count);
            Assert.Equal("SELECT /* a;b */ 1", parts[0]);
            Assert.Equal("SELECT 2", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolon_inside_nested_block_comment_is_not_a_separator()
        {
            // PG block comments nest: the inner */ closes only the inner comment, so the ; stays
            // inside the still-open outer comment and must not split.
            var parts = SqlStatementSplitter.Split("SELECT /* outer /* inner */ still;here */ 1");
            Assert.Single(parts);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Escaped_single_quote_inside_string_does_not_close_the_literal()
        {
            // PG escapes a single quote inside a string by doubling it: 'it''s'.
            var parts = SqlStatementSplitter.Split("SELECT 'it''s; ok'; SELECT 2");
            Assert.Equal(2, parts.Count);
            Assert.Equal("SELECT 'it''s; ok'", parts[0]);
            Assert.Equal("SELECT 2", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Dollar_quoted_strings_protect_semicolons_within()
        {
            var parts = SqlStatementSplitter.Split("SELECT $tag$a;b;c$tag$; SELECT 2");
            Assert.Equal(2, parts.Count);
            Assert.Equal("SELECT $tag$a;b;c$tag$", parts[0]);
            Assert.Equal("SELECT 2", parts[1]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Digit_first_dollar_tag_is_not_a_dollar_quote()
        {
            // $1 is a positional parameter, not a dollar-quote tag (tags follow identifier rules and
            // can't start with a digit), so $1$ doesn't open a quote and the ; inside is a separator.
            var parts = SqlStatementSplitter.Split("SELECT $1$a;b$1$");
            Assert.Equal(2, parts.Count);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Empty_statements_between_semicolons_are_dropped()
        {
            var parts = SqlStatementSplitter.Split("SELECT 1;;; SELECT 2;");
            Assert.Equal(2, parts.Count);
        }

        // ── RQL pass-through ──────────────────────────────────────────────────────────
        //
        // RQL queries have no statement-separator semantics (no `;` between statements on the
        // wire) but they routinely contain `;` inside `declare function { ... }` JavaScript
        // bodies. The splitter must recognize RQL by its leading keyword and pass the whole
        // input through as a single statement — otherwise PowerBI / pgAdmin users hitting a
        // `select output(x)`-style query with a JS helper get the function body shredded
        // mid-statement and the query errors with "Unhandled query".

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_declare_function_with_semicolons_in_body_is_not_split()
        {
            const string rql = """
                declare function output(usage) {
                    var r = usage.ModelLog.Response.filter(y => y.ModelId == usage.Id);
                    return { Id : usage.ModelId, Response: r[0] };
                }

                from index 'UsageByModel' as x
                select output(x)
                """;

            var parts = SqlStatementSplitter.Split(rql);
            Assert.Single(parts);
            Assert.Contains("declare function output", parts[0]);
            Assert.Contains("from index 'UsageByModel'", parts[0]);
            Assert.Contains("select output(x)", parts[0]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_plain_from_query_is_passed_through_unsplit()
        {
            var parts = SqlStatementSplitter.Split("from Orders where Freight > 50");
            Assert.Single(parts);
            Assert.Equal("from Orders where Freight > 50", parts[0]);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_with_trailing_semicolon_is_passed_through_unsplit()
        {
            // The user-supplied trailing `;` would normally trigger a split into [query, ""].
            // With RQL pass-through, the whole text including the trailing `;` survives — the
            // downstream RQL parser will reject the trailing `;` (that's invalid RQL) but the
            // splitter's job is to keep the input intact, not to normalize it.
            var parts = SqlStatementSplitter.Split("from Orders;");
            Assert.Single(parts);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_leading_whitespace_does_not_break_detection()
        {
            var parts = SqlStatementSplitter.Split("   \n   from Orders");
            Assert.Single(parts);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_leading_line_comment_does_not_break_detection()
        {
            const string rql = """
                -- pulled from PowerBI's "Get Data" advanced editor
                from Orders
                """;

            var parts = SqlStatementSplitter.Split(rql);
            Assert.Single(parts);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rql_leading_block_comment_does_not_break_detection()
        {
            const string rql = """
                /* fetched from index */
                from index 'Orders/ByFreight' where Freight > 50
                """;

            var parts = SqlStatementSplitter.Split(rql);
            Assert.Single(parts);
        }

        // Negative pin: SQL queries beginning with SELECT must STILL go through the splitter
        // (so multi-statement batches like pgAdmin's startup probe keep working).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Sql_select_with_semicolons_continues_to_split()
        {
            var parts = SqlStatementSplitter.Split("SELECT 1; SELECT 2; SELECT 3");
            Assert.Equal(3, parts.Count);
        }

        // Negative pin: `fromX` (identifier starting with `from`) must NOT be treated as RQL.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Sql_identifier_starting_with_from_does_not_trigger_passthrough()
        {
            // `fromage` is a perfectly valid (if unusual) SQL identifier. The word-boundary
            // check on the RQL detector must reject this so the SQL splitter handles it.
            var parts = SqlStatementSplitter.Split("SELECT fromage FROM cheeses; SELECT 1");
            Assert.Equal(2, parts.Count);
        }

        // PowerBI Desktop's schema-discovery probe wraps user-provided RQL as
        // `select * from (USER_RQL) "_" limit 0`. If USER_RQL contains a `declare function {...}`
        // body, the JS body has semicolons. The outer text starts with `select` (not `declare`/`from`),
        // so the RQL passthrough doesn't apply — meaning the splitter has to NOT split on `;` that
        // are inside the outer `(...)` (or inside the JS body's `{...}`).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Powerbi_wrapped_rql_with_declare_function_is_not_split()
        {
            const string wrapped = """
                select * from (
                    declare function output(usage) {
                        var r = usage.ModelLog.Response.filter(y => y.Id == usage.ModelId);
                        return { Id: usage.ModelId, Response: r[0] };
                    }
                    from 'Usages' as x
                    select output(x)
                ) "_" limit 0
                """;

            var parts = SqlStatementSplitter.Split(wrapped);
            Assert.Single(parts);
            Assert.Contains("declare function output", parts[0]);
            Assert.Contains("from 'Usages'", parts[0]);
        }

        // Brace-depth tracking must also preserve `;` inside a top-level `{...}` block.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolons_inside_top_level_brace_block_do_not_split()
        {
            const string sql = "FOO { var a = 1; var b = 2; } BAR";

            var parts = SqlStatementSplitter.Split(sql);
            Assert.Single(parts);
        }

        // Paren-depth tracking must also preserve `;` inside nested parens.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Semicolons_inside_parens_do_not_split()
        {
            const string sql = "SELECT * FROM (SELECT 1; SELECT 2) x";

            var parts = SqlStatementSplitter.Split(sql);
            Assert.Single(parts);
        }
    }
}
