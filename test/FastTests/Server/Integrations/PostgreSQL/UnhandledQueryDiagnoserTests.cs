using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // Pins UnhandledQueryDiagnoser's per-shape "why unsupported" messages and the shapes it must not flag.
    public sealed class UnhandledQueryDiagnoserTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Join_OverUserCollections_DetectedWithLoadIncludeHint()
        {
            var sql = """
                SELECT o."Company", e."LastName"
                FROM "public"."Orders" o
                JOIN "public"."Employees" e ON o."Employee" = e."id"
                LIMIT 5
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("JOIN", message);
            Assert.Contains("load", message);
            Assert.Contains("include", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Join_LeftOuter_AlsoDetected()
        {
            var sql = """
                SELECT o."Company"
                FROM "public"."Orders" o
                LEFT OUTER JOIN "public"."Employees" e ON o."Employee" = e."id"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        // PowerBI wraps the user's SQL as `SELECT * FROM (USER_SQL) "_" LIMIT N`, so a JOIN ends up
        // nested inside the wrapper — the diagnoser has to look inside it to still detect the JOIN.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Join_InsidePowerBiWrapper_IsDetected()
        {
            var sql = """
                SELECT *
                FROM (
                    SELECT o."Company", e."LastName"
                    FROM "public"."Orders" o
                    JOIN "public"."Employees" e ON o."Employee" = e."id"
                ) "_"
                LIMIT 1000001
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("JOIN", message);
            Assert.Contains("load", message);
        }

        // Doubly-wrapped shape that some PowerBI variants emit — JOIN two levels deep.
        // Bounded recursion must still find it.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Join_InsideNestedWrapper_IsDetected()
        {
            var sql = """
                SELECT *
                FROM (
                    SELECT *
                    FROM (
                        SELECT a, b
                        FROM "public"."T1"
                        JOIN "public"."T2" ON T1.id = T2.id
                    ) "inner"
                ) "outer"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        // Negative pin: a wrapper that doesn't contain a JOIN inside must NOT be classified as
        // JOIN — the diagnoser would otherwise misclassify every PowerBI-wrapped query.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Wrapper_WithoutJoinInside_IsNotClassifiedAsJoin()
        {
            var sql = """
                SELECT "Company", "Freight"
                FROM (SELECT "Company", "Freight" FROM "public"."Orders") "_"
                LIMIT 100
                """;

            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarSum_WithoutGroupBy_DetectedWithGroupByOrClientSideHint()
        {
            var sql = """SELECT sum("Freight") FROM "public"."Orders" """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("Scalar aggregate", message);
            Assert.Contains("GROUP BY", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarCount_WithoutGroupBy_AlsoDetected()
        {
            var sql = """SELECT count(*) FROM "public"."Orders" """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarAggregate_QualifiedFuncName_AlsoDetected()
        {
            // PowerBI sometimes emits qualified aggregate names like `pg_catalog.sum(...)`;
            // the diagnoser must match the last segment of the function-name path.
            var sql = """SELECT pg_catalog.sum("Freight") FROM "public"."Orders" """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        // GROUP BY + aggregate IS supported (translator handles it); we must NOT classify it as
        // scalar-aggregate-unsupported.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupedAggregate_IsNotClassifiedAsScalarAggregate()
        {
            var sql = """
                SELECT "Company", sum("Freight") AS total
                FROM "public"."Orders"
                GROUP BY "Company"
                """;

            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PlainSelect_NoClassification()
        {
            var sql = """SELECT "Company", "Freight" FROM "public"."Orders" LIMIT 5""";

            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        // Mixed shapes (one bare column + one aggregate without GROUP BY) are a SQL error in
        // real PG. We don't classify them as scalar-aggregate — the translator emits its own
        // clearer "mixing aggregates and non-aggregated columns" error for these.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void MixedAggregateAndBareColumn_NoClassification()
        {
            var sql = """SELECT "Company", sum("Freight") FROM "public"."Orders" """;

            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Unparseable_NoClassification()
        {
            Assert.False(UnhandledQueryDiagnoser.TryDiagnose("not valid sql at all $$$", out _));
        }

        // PowerBI's PostgreSQL connector splits the M `Query=` value on `;` client-side, so an
        // RQL `declare function {...; ...}` arrives as just the first piece — unbalanced braces,
        // unparseable. Diagnoser must catch this and tell the user to remove the semicolons instead of
        // dumping the fragment with a generic "Unhandled query".
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void JsBodyFragment_FromPowerBiSemicolonSplit_SuggestsRemovingSemicolons()
        {
            const string fragment = "declare function output(usage) { var r = usage.ModelLog.Response.filter(y => y.Id == usage.ModelId)";

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(fragment, out var message));
            Assert.Contains("fragment", message);
            Assert.Contains("declare function", message);
            Assert.Contains("semicolons", message);
        }

        // A complete `declare function { ... }` RQL with balanced braces should NOT be
        // classified — that's valid RQL and would dispatch through RqlQuery.TryParse normally.
        // Diagnoser only fires when both arms of the parse fail; this query parses as RQL.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void CompleteDeclareFunction_NotClassifiedAsFragment()
        {
            const string complete = """
                declare function output(u) {
                    var r = u.ModelLog.Response.filter(y => y.Id == u.ModelId)
                    return { Id: u.ModelId, Response: r[0] }
                }

                from 'Usages' as x select output(x)
                """;

            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(complete, out _));
        }

        // INTERSECT / EXCEPT are not implemented in the virtual catalog interpreter (only UNION is).
        // The diagnoser must catch these and point at the limitation instead of dumping the SQL.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Intersect_Detected()
        {
            const string sql = """
                SELECT id FROM "public"."Orders"
                INTERSECT
                SELECT id FROM "public"."Companies"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("INTERSECT", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Except_Detected()
        {
            const string sql = """
                SELECT id FROM "public"."Orders"
                EXCEPT
                SELECT id FROM "public"."Companies"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("EXCEPT", message);
        }

        // UNION must continue to NOT be classified as an unhandled set op — the virtual
        // catalog actually supports it.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Union_NotClassifiedAsUnhandledSetOp()
        {
            const string sql = """
                SELECT id FROM "public"."Orders"
                UNION ALL
                SELECT id FROM "public"."Companies"
                """;

            // Diagnoser may return false (no targeted message) — but if it does return true,
            // the message must NOT mention INTERSECT/EXCEPT.
            if (UnhandledQueryDiagnoser.TryDiagnose(sql, out var message))
            {
                Assert.DoesNotContain("INTERSECT", message);
                Assert.DoesNotContain("EXCEPT", message);
            }
        }

        // SQL queries without "declare function" anywhere must not get the fragment classification.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void PlainSql_NotClassifiedAsFragment()
        {
            Assert.False(UnhandledQueryDiagnoser.TryDiagnose("SELECT * FROM unknown_table", out _));
        }

        // A SQL query that happens to mention "declare function" inside a string literal must
        // NOT be classified as a fragment — only queries that actually START with `declare
        // function` are real RQL fragments.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void DeclareFunctionInStringLiteral_NotClassifiedAsFragment()
        {
            const string sql = "SELECT * FROM \"docs\" WHERE \"note\" = 'we declare function {x' AND \"id\" > 0";
            Assert.False(UnhandledQueryDiagnoser.TryDiagnose(sql, out _));
        }

        // A `declare function` body that contains a JS string literal with a stray `}` (e.g.
        // `return "}"`) must still be classified as a fragment if the outer body is truncated
        // — brace counting must skip the string contents so the literal's `}` doesn't balance
        // the body's real `{`.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void JsBodyFragment_WithBraceInStringLiteral_StillDetected()
        {
            const string fragment = "declare function f(x) { var y = \"}\"; var z = x.foo";
            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(fragment, out var message));
            Assert.Contains("fragment", message);
        }

        // A JS body containing single-quoted strings with stray `{` should still be detected
        // as a fragment when braces are actually unbalanced — single-quote skipping mustn't
        // accidentally swallow real braces outside the string.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void JsBodyFragment_WithSingleQuotedString_StillDetected()
        {
            const string fragment = "declare function f(x) { var y = 'has-an-open-{-inside'; var z = x.foo";
            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(fragment, out _));
        }

        // min()/max() aggregates aren't supported by RavenDB's map-reduce engine — the
        // AggregationOperation enum models only Count and Sum. The diagnoser must catch this
        // (both with and without GROUP BY) and point at the ORDER BY + LIMIT 1 workaround.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void MinAggregate_WithGroupBy_Detected()
        {
            var sql = """
                SELECT "Company", min("Freight") AS "m"
                FROM "public"."Orders"
                GROUP BY "Company"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("min()", message);
            Assert.Contains("max()", message);
            Assert.Contains("ORDER BY", message);
            Assert.Contains("LIMIT 1", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void MaxAggregate_WithoutGroupBy_Detected()
        {
            // Bare scalar `SELECT max(x) FROM t` is doubly unsupported (no GROUP BY AND uses
            // max). The min/max diagnostic must win because its workaround is more useful.
            var sql = """SELECT max("Freight") FROM "public"."Orders" """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("max()", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void MixedMinAndSum_Detected()
        {
            // If any projection is min/max we still produce the min/max-specific message —
            // explaining only "scalar aggregate without GROUP BY" would mislead since adding a
            // GROUP BY wouldn't help.
            var sql = """
                SELECT "Company", min("Freight"), sum("Freight")
                FROM "public"."Orders"
                GROUP BY "Company"
                """;

            Assert.True(UnhandledQueryDiagnoser.TryDiagnose(sql, out var message));
            Assert.Contains("min()", message);
        }
    }
}
