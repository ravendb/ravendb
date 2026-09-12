using System;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.Translation
{
    public sealed class PgSqlToRqlTranslatorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private static string Translate(string sql)
        {
            Assert.True(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out var rql));
            return rql;
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectAllFromUsers()
        {
            var sql = "SELECT * FROM users";
            var expected = "from 'users'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void IndexQueryWithSlashInName_Translates()
        {
            // RavenDB index names legitimately contain '/'; they must translate, not be rejected.
            Assert.Equal("from index 'Orders/Totals'", Translate("SELECT * FROM \"indexes\".\"Orders/Totals\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void IndexNameWithQuote_FallsThrough()
        {
            // A quote/backslash in the index name would break the emitted `from index '...'` literal
            // (the collection path is escaped by FromToken; the index path is not) - reject it.
            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                "SELECT * FROM \"indexes\".\"Bad'Name\"", Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereAmountGreaterThan()
        {
            var sql = "SELECT * FROM orders WHERE amount > 10";
            var expected = "from 'orders' where amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereNameEqualsString()
        {
            var sql = "SELECT * FROM users WHERE name = 'ayende'";
            var expected = "from 'users' where name = 'ayende'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereStringEqualsEmpty_PreservesEmptyLiteral()
        {
            var sql = "SELECT * FROM users WHERE name = ''";
            var expected = "from 'users' where name = ''";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereActiveTrue()
        {
            var sql = "SELECT * FROM users WHERE active = true";
            var expected = "from 'users' where active = true";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereStatusNotEqualsString()
        {
            var sql = "SELECT * FROM orders WHERE status <> 'Cancelled'";
            var expected = "from 'orders' where status != 'Cancelled'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereAnd()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' AND amount > 10";
            var expected = "from 'orders' where status = 'Pending' and amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereOr()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' OR status = 'Shipped'";
            var expected = "from 'orders' where status = 'Pending' or status = 'Shipped'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByDesc()
        {
            var sql = "SELECT * FROM users ORDER BY name DESC";
            var expected = "from 'users' order by name desc";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void LimitOffset()
        {
            var sql = "SELECT * FROM users LIMIT 10 OFFSET 20";
            var expected = "from 'users' limit 20, 10";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByDescLimit()
        {
            // Unquoted identifiers follow PostgreSQL semantics (folded to lowercase).
            // Users who need exact RavenDB field casing must quote the identifier.
            var sql = "SELECT * FROM orders ORDER BY createdAt LIMIT 5";
            var expected = "from 'orders' order by createdat limit 0, 5";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByNonColumnExpression_IsRejected()
        {
            // A sort key that isn't a plain column has no RQL form here; bail so the diagnoser
            // fires instead of silently dropping it and returning mis-ordered rows.
            var sql = "SELECT * FROM users ORDER BY upper(name)";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void QuotedExplicitProjectionWithLimit_EmitsLimitClause()
        {
            var sql = """
                SELECT "Company", "Freight" AS "shipping_cost", "OrderedAt" AS "placed"
                FROM "public"."Orders"
                LIMIT 5
                """;
            var expected = "from 'Orders' select Company, Freight as shipping_cost, OrderedAt as placed limit 0, 5";
            Assert.Equal(expected, Translate(sql));
        }

        // NOT IN: RavenDB's client query API has no WhereNotIn, so the translator flips polarity via
        // NegateNext() on the following WhereIn. NegateNext() also prepends an `exists(<field>)` clause
        // so the negation is null-safe (RQL: rows missing the field don't match a negated predicate;
        // matches PG's NOT IN semantics).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void NotIn_FlipsViaNegateNext()
        {
            var sql = @"SELECT ""Company"" FROM ""public"".""Orders"" WHERE ""Freight"" NOT IN (1.21, 1.35) LIMIT 5";
            var expected = "from 'Orders' where exists(Freight) and not Freight in (1.21, 1.35) select Company limit 0, 5";

            Assert.Equal(expected, Translate(sql));
        }

        // General NOT around a primitive predicate: NegateNext() flips the next emitted predicate.
        // Compound NOTs (e.g. NOT(a AND b)) still throw - see the ParsedNot guard in PgSqlToRqlTranslator.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void NotBinary_FlipsViaNegateNext()
        {
            var sql = @"SELECT ""Company"" FROM ""public"".""Orders"" WHERE NOT (""Freight"" > 50) LIMIT 5";
            var expected = "from 'Orders' where exists(Freight) and not Freight > 50 select Company limit 0, 5";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereDottedPathEquals()
        {
            var sql = "SELECT * FROM orders WHERE ShipTo.City = 'London'";
            var expected = "from 'orders' where shipto.city = 'London'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereBetween()
        {
            var sql = "SELECT * FROM orders WHERE amount BETWEEN 10 AND 20";
            var expected = "from 'orders' where amount between 10 and 20";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereInList()
        {
            var sql = "SELECT * FROM orders WHERE status IN ('Pending','Shipped')";
            var expected = "from 'orders' where status in ('Pending', 'Shipped')";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereInListOnDottedPath()
        {
            var sql = "SELECT * FROM orders WHERE shipTo.city IN ('London','Paris')";
            var expected = "from 'orders' where shipto.city in ('London', 'Paris')";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereParenthesizedOrAndedWithComparison()
        {
            var sql = "SELECT * FROM orders WHERE (status = 'Pending' OR status = 'Shipped') AND amount > 10";
            var expected = "from 'orders' where (status = 'Pending' or status = 'Shipped') and amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByTwoFields()
        {
            var sql = "SELECT * FROM orders ORDER BY createdAt DESC, amount ASC";
            var expected = "from 'orders' order by createdat desc, amount";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereOrderByLimit()
        {
            var sql = "SELECT * FROM users WHERE name <> 'oren' ORDER BY name LIMIT 20";
            var expected = "from 'users' where name != 'oren' order by name limit 0, 20";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void AndWithDottedPath()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' AND shipTo.city = 'London'";
            var expected = "from 'orders' where status = 'Pending' and shipto.city = 'London'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereIsNull()
        {
            var sql = "SELECT * FROM orders WHERE shippedAt IS NULL";
            var expected = "from 'orders' where shippedat = null";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void AndWithParenthesizedOr()
        {
            var sql = "SELECT * FROM users WHERE active = true AND (name = 'ayende' OR name = 'oren')";
            var expected = "from 'users' where active = true and (name = 'ayende' or name = 'oren')";

            Assert.Equal(expected, Translate(sql));
        }

        // The PG endpoint exposes the document identifier as `id` (PG-idiomatic; see
        // PgSyntheticColumns), but under the hood it's RQL's `id()` function. The translator
        // maps both `id` and `id()` in user SQL to `id()` so the engine reads the doc identifier.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectColumns()
        {
            var sql = "SELECT id, name FROM users";
            var expected = "from 'users' select id(), name";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectColumnsWithWhere()
        {
            var sql = "SELECT id, status, shipTo.city FROM orders WHERE amount > 10";
            var expected = "from 'orders' where amount > 10 select id(), status, shipto.city";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarCountStar_IsRejected()
        {
            // Scalar count(*) with no GROUP BY has no valid RQL form (the engine rejects
            // `from t select count()` with "count may only be used in group by queries").
            // The translator bails so PgQuery surfaces a friendly diagnoser message instead.
            var sql = "SELECT COUNT(*) FROM orders";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Injection_via_quoted_field_name_in_where_is_rejected()
        {
            // A SQL identifier carrying ' or \ could break out of RQL's single-quoted string (the builder
            // wraps other characters but doesn't escape these). Reject - fall through to the diagnoser.
            var sql = "SELECT * FROM users WHERE \"x' OR '1'='1\" = 'z'";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Injection_via_quoted_select_alias_is_rejected()
        {
            // An alias carrying ' or \ is rejected rather than spliced (same break-out risk as fields).
            var sql = "SELECT name AS \"a' OR '1\" FROM users";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereField_WithSpaces_IsQuotedNotRejected()
        {
            // A field name with spaces has no break-out characters, so it is quoted (via the builder)
            // rather than rejected.
            Assert.Equal("from 'users' where 'Unit Price' > 10", Translate("SELECT * FROM users WHERE \"Unit Price\" > 10"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectField_WithSpaces_IsQuoted()
        {
            Assert.Equal("from 'users' select 'Unit Price'", Translate("SELECT \"Unit Price\" FROM users"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectAlias_WithSpaces_IsQuoted()
        {
            Assert.Equal("from 'users' select name as 'a b'", Translate("SELECT name AS \"a b\" FROM users"));
        }

        // PowerBI's row-preview queries decorate the projection with constant markers (e.g.
        // `1 as "c0"`) to count back a fixed shape. The translator must forward the literal and
        // its alias - dropping the column gives PowerBI a "Field count mismatch" error.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_IntegerProjection_WithAlias_PreservesLiteralAndAlias()
        {
            var sql = "SELECT name, 1 AS \"c0\" FROM users";
            var expected = "from 'users' select name, 1 as c0";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_StringProjection_WithAlias_QuotesValueAndPreservesAlias()
        {
            var sql = "SELECT name, 'literal' AS \"marker\" FROM users";
            var expected = "from 'users' select name, 'literal' as marker";

            Assert.Equal(expected, Translate(sql));
        }

        // Single-quote inside a string literal must double up - RQL uses the same escape
        // convention as SQL/PG. Otherwise `'O''Brien' as note` breaks RQL parsing.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_StringWithSingleQuote_EscapesByDoubling()
        {
            var sql = "SELECT name, 'O''Brien' AS \"note\" FROM users";
            var expected = "from 'users' select name, 'O''Brien' as note";

            Assert.Equal(expected, Translate(sql));
        }

        // RQL's scanner treats backslash as an escape character inside single-quoted strings, so a
        // backslash in a SQL string value must be doubled when emitted as an RQL literal. Without
        // this, `WHERE name = 'a\b'` emits `'a\b'`, which RQL decodes as `a` + backspace (silent
        // value corruption), and a crafted value can terminate the literal early and inject RQL
        // once the emitted query is re-parsed.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void WhereStringWithBackslash_EscapesBackslashByDoubling()
        {
            var sql = "SELECT * FROM users WHERE name = 'a\\b'";
            var expected = "from 'users' where name = 'a\\\\b'";

            Assert.Equal(expected, Translate(sql));
        }

        // Same backslash-escaping requirement on the const-projection path (TryRenderRqlLiteral),
        // which now shares the WHERE translator's QuoteString helper.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_StringWithBackslash_EscapesByDoubling()
        {
            var sql = "SELECT name, 'a\\b' AS \"note\" FROM users";
            var expected = "from 'users' select name, 'a\\\\b' as note";

            Assert.Equal(expected, Translate(sql));
        }

        // ORDER BY on the grouping key when that key isn't in the SELECT list
        // (`SELECT sum(Freight) ... GROUP BY Company ORDER BY Company`) must fall through cleanly
        // (TryParse returns false) rather than throwing from a First() with no matching projection.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByOrderByOnNonProjectedKey_FailsGracefullyWithoutThrowing()
        {
            var sql = "SELECT sum(Freight) FROM Orders GROUP BY Company ORDER BY Company";

            var translated = Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _);

            Assert.False(translated);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_BooleanProjection_PreservesAsRqlBoolean()
        {
            var sql = "SELECT name, true AS \"flag\" FROM users";
            var expected = "from 'users' select name, true as flag";

            Assert.Equal(expected, Translate(sql));
        }

        // A literal without an explicit AS alias falls through to the field expression itself,
        // matching the existing single-arg SelectFields semantics. RQL accepts `select 1`
        // (auto-naming the column in the result).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ConstLiteral_IntegerProjection_WithoutAlias_OmitsAsClause()
        {
            var sql = "SELECT name, 1 FROM users";
            var expected = "from 'users' select name, 1";

            Assert.Equal(expected, Translate(sql));
        }

        // RQL's count() ignores its argument, so emitting count(Freight) returned the group's row
        // count while PG counts non-null values only. The PowerBI dispatch path normalises the same
        // shape to count() on purpose (see PowerBIAstTests); the generic SQL path must not guess.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_CountOverAliasQualifiedColumn_IsRejected()
        {
            var sql = """
                select "rows"."Freight" as "Freight", count("rows"."Freight") as "a0"
                from "public"."Orders" "rows"
                group by "Freight"
                limit 1000001
                """;

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_WithFromAlias_SumWithAliasQualifiedArg_PreservesAggregateAlias()
        {
            var sql = """
                select "rows"."Company" as "Company", sum("rows"."Freight") as "a0"
                from "public"."Orders" "rows"
                group by "Company"
                """;
            var expected = "from 'Orders' group by Company select Company, sum(Freight) as a0";

            Assert.Equal(expected, Translate(sql));
        }

        // Scalar aggregates (all-aggregate SELECT, no GROUP BY) have no valid RQL form - the engine
        // rejects `from t select sum(x)`. The translator bails so PgQuery falls through to
        // UnhandledQueryDiagnoser for a friendly message instead of RQL that fails at execution.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarAggregates_AreRejected()
        {
            var sql = "SELECT COUNT(*), SUM(amount), AVG(score) FROM orders";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ScalarAvgWithWhere_IsRejected()
        {
            var sql = "SELECT AVG(amount) FROM orders WHERE status = 'Paid'";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCount()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";
            var expected = "from 'orders' group by status select status, count()";
            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountOrderByCountDesc()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*) DESC";
            var expected = "from 'orders' group by status order by Count as long desc select status, count()";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountOrderByCountAsc()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*)";
            var expected = "from 'orders' group by status order by Count as long select status, count()";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountOrderByAggregateAlias()
        {
            var sql = "SELECT \"Company\" AS \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY count DESC LIMIT 10000";
            var expected = "from 'Orders' group by Company order by Count as long desc select Company, count() as count limit 0, 10000";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBySumOrderBySumDesc()
        {
            var sql = "SELECT \"Company\", sum(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY sum(\"Freight\") DESC";
            var expected = "from 'Orders' group by Company order by Freight as double desc select Company, sum(Freight) as a0";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBySumOrderByAggregateAlias()
        {
            var sql = "SELECT \"Company\", sum(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY \"a0\"";
            var expected = "from 'Orders' group by Company order by Freight as double select Company, sum(Freight) as a0";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountOrderByGroupKey()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status DESC";
            var expected = "from 'orders' group by status order by status desc select status, count()";

            Assert.Equal(expected, Translate(sql));
        }

        // sum() over the group key resolves to the key's own RQL field name, so ordering by it
        // would sort by the key instead of the aggregate.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByOrderBySumOfGroupKey_IsRejected()
        {
            var sql = "SELECT \"Freight\", sum(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Freight\" ORDER BY \"a0\" DESC";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByOrderByAggregateNotInSelect_IsRejected()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY sum(amount) DESC";

            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectDistinct()
        {
            var sql = "SELECT DISTINCT status FROM orders";
            var expected = "from 'orders' select distinct status";
            Assert.Equal(expected, Translate(sql));
        }

        // PowerBI's distinct-values probe (slicer / filter-dropdown options) sends
        // `SELECT col1, col2 ... GROUP BY col1, col2` instead of `SELECT DISTINCT`. For multi-column
        // shapes we emit `group by ... select ...` because RQL's `select distinct` dedupes by
        // first-field only, leaving duplicate tuples that break PowerBI's mashup engine.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_WithoutAggregates_TwoColumns_TranslatesToGroupByDistinct()
        {
            var sql = "SELECT status, region FROM orders GROUP BY status, region";
            var expected = "from 'orders' group by status, region select status, region";
            Assert.Equal(expected, Translate(sql));
        }

        // The exact shape PowerBI Desktop fires for a two-column slicer probe against a
        // `public.X` table: wrapper alias on the source, both columns aliased, and PowerBI's
        // 1,000,001-row sentinel limit. Pinned so the recognizer dispatch
        // (PowerBIFetchQuery -> PgSqlToRqlTranslator) keeps working end-to-end.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_PowerBI_DistinctValuesProbe_AliasedSource_TranslatesToGroupBy()
        {
            var sql = """
                select "rows"."Company" as "Company", "rows"."Freight" as "Freight"
                from "public"."Orders" "rows"
                group by "Company", "Freight"
                limit 1000001
                """;

            var rql = Translate(sql);

            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("group by Company, Freight", rql, StringComparison.Ordinal);
            Assert.Contains("select Company, Freight", rql, StringComparison.Ordinal);
            Assert.Contains("limit 0, 1000001", rql, StringComparison.OrdinalIgnoreCase);
        }

        // Single-column-without-aggregate must keep working - same path as SELECT DISTINCT, just
        // via the GROUP BY surface.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_WithoutAggregates_SingleColumn_TranslatesToDistinct()
        {
            var sql = "SELECT status FROM orders GROUP BY status";
            var expected = "from 'orders' select distinct status";
            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void SelectFromIndex()
        {
            var sql = "SELECT * FROM indexes.\"Users/ByName\" WHERE name = 'oren'";
            var expected = "from index 'Users/ByName' where name = 'oren'";

            Assert.Equal(expected, Translate(sql));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void InnerJoin()
        {
            var sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
            var expected = "from 'orders' as o load o.user_id as u select { o: o, u: u }";

            Assert.Equal(expected, Translate(sql));
        }

        // Identifier case handling. Unquoted identifiers follow PostgreSQL semantics:
        // pgsqlparser folds them to lowercase before the AST is built. Quoted identifiers
        // preserve case. Users who need exact RavenDB field casing must quote the identifier.

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void IdentifierCasing_QuotedIdentifier_PreservesCase()
        {
            var sql = "SELECT \"Company\" FROM orders WHERE \"Title\" = 'Manager'";
            var rql = Translate(sql);

            Assert.Contains("Company", rql, StringComparison.Ordinal);
            Assert.Contains("Title", rql, StringComparison.Ordinal);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void IdentifierCasing_UnquotedIdentifier_FoldedToLowercase()
        {
            var sql = "SELECT Company FROM orders WHERE Title = 'Manager'";
            var rql = Translate(sql);

            // Per PostgreSQL semantics: unquoted folded to lowercase.
            Assert.Contains("company", rql, StringComparison.Ordinal);
            Assert.DoesNotContain("Company", rql, StringComparison.Ordinal);
            Assert.Contains("title", rql, StringComparison.Ordinal);
            Assert.DoesNotContain("Title", rql, StringComparison.Ordinal);
        }

        // PowerBI incremental refresh / date-range filters.
        //
        // PowerBI's incremental refresh turns the RangeStart/RangeEnd parameters into a
        // parameterized date-range predicate on a DateTime column: `WHERE "col" >= $1 AND
        // "col" < $2`, bound at Bind time via the Extended Query Protocol. The tests below cover
        // the inline `timestamp 'X'` literal, the `'X'::timestamp` cast, and the parameterized form.

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void DateRange_InlineTimestampLiteral_TranslatesBothBounds()
        {
            // `timestamp 'X'` is PG-idiomatic typed-literal syntax - emits TypeCast in the AST.
            var sql = """SELECT * FROM orders WHERE "OrderedAt" >= timestamp '1996-08-01' AND "OrderedAt" < timestamp '1996-09-01'""";
            var rql = Translate(sql);

            Assert.Contains("from 'orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            // Both bounds must reach the emitted RQL or RavenDB's auto-index scans the whole
            // collection per partition.
            Assert.Contains("1996-08", rql, StringComparison.Ordinal);
            Assert.Contains("1996-09", rql, StringComparison.Ordinal);
            Assert.Contains(">=", rql, StringComparison.Ordinal);
            Assert.Contains("<", rql, StringComparison.Ordinal);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void DateRange_CastTimestampLiteral_TranslatesBothBounds()
        {
            // `'X'::timestamp` - same TypeCast, different source ordering.
            var sql = """SELECT * FROM orders WHERE "OrderedAt" >= '1996-08-01'::timestamp AND "OrderedAt" < '1996-09-01'::timestamp""";
            var rql = Translate(sql);

            Assert.Contains("from 'orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            Assert.Contains("1996-08", rql, StringComparison.Ordinal);
            Assert.Contains("1996-09", rql, StringComparison.Ordinal);
        }

        // PowerBI emits this shape for incremental-refresh windows: quoted column + half-open
        // range. Asserts TryParse succeeds and both bounds reach the emitted RQL.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void DateRange_QuotedColumnHalfOpenWindow_TranslatesToRangeFilter()
        {
            var sql = """
                SELECT "OrderedAt", "Freight" FROM "Orders"
                WHERE "OrderedAt" >= timestamp '2024-01-01' AND "OrderedAt" < timestamp '2024-02-01'
                ORDER BY "OrderedAt"
                """;
            var rql = Translate(sql);

            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            Assert.Contains("Freight", rql, StringComparison.Ordinal);
            Assert.Contains("2024-01", rql, StringComparison.Ordinal);
            Assert.Contains("2024-02", rql, StringComparison.Ordinal);
        }

        // The shape PowerBI actually sends for incremental refresh. A $N placeholder in a WHERE
        // value can't be inlined at translate time (Parse precedes Bind in the Extended Query
        // Protocol), so the translator emits an RQL parameter reference instead. The 1-based PG
        // index maps straight through: SQL $1/$2 -> RQL $1/$2, which PgQuery.Bind then fills in.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void DateRange_ParameterizedBounds_TranslatesWithParamRefs()
        {
            var sql = """SELECT * FROM "Orders" WHERE "OrderedAt" >= $1 AND "OrderedAt" < $2""";
            var rql = Translate(sql);

            Assert.Contains("from 'Orders'", rql, StringComparison.Ordinal);
            Assert.Contains("OrderedAt", rql, StringComparison.Ordinal);
            // The PG parameter index doubles as the RQL parameter name (RQL allows numeric
            // names), so the placeholders survive translation as $1 / $2 rather than being
            // inlined as literals we don't have yet.
            Assert.Contains("$1", rql, StringComparison.Ordinal);
            Assert.Contains("$2", rql, StringComparison.Ordinal);
        }

        // LIKE is case-sensitive, so it needs exact() to reach the auto index's unanalyzed companion
        // field; ILIKE is what the analyzed field already does.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Like_Prefix_TranslatesToExactStartsWith()
        {
            Assert.Equal("from 'orders' where exact(startsWith(company, 'Choc'))",
                Translate("SELECT * FROM orders WHERE company LIKE 'Choc%'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Ilike_Prefix_TranslatesToStartsWith()
        {
            Assert.Equal("from 'orders' where startsWith(company, 'Choc')",
                Translate("SELECT * FROM orders WHERE company ILIKE 'Choc%'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Like_Suffix_TranslatesToExactEndsWith()
        {
            Assert.Equal("from 'orders' where exact(endsWith(company, 'ade'))",
                Translate("SELECT * FROM orders WHERE company LIKE '%ade'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Like_WithoutWildcards_TranslatesToExactEquality()
        {
            Assert.Equal("from 'orders' where exact(company = 'Chocolade')",
                Translate("SELECT * FROM orders WHERE company LIKE 'Chocolade'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void NotLike_GuardsAgainstNull()
        {
            Assert.Equal("from 'orders' where (company != null and not exact(startsWith(company, 'Choc')))",
                Translate("SELECT * FROM orders WHERE company NOT LIKE 'Choc%'"));
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT * FROM orders WHERE company LIKE '%Choc%'")]
        [InlineData("SELECT * FROM orders WHERE company ILIKE '%Choc%'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE 'C_ocolade'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE 'Choc%ade'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE 'Choc\\%ade'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE '%'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE '!%Choc%' ESCAPE '!'")]
        [InlineData("SELECT * FROM orders WHERE company LIKE $1")]
        public void UnsupportedLikePattern_FallsThrough(string sql)
        {
            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                sql, Array.Empty<int>(), out _));
        }

        // DISTINCT / FILTER / OVER and count(<column>) all used to translate to the plain aggregate,
        // which RQL computes over every row in the group - the same number as count(*)/sum(*).
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Company\", count(distinct \"ShipVia\") FROM \"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(\"ShipVia\") FROM \"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", sum(distinct \"Freight\") FROM \"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(*) FILTER (WHERE \"Freight\" > 10) FROM \"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(*) OVER () FROM \"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(*) FROM \"Orders\" GROUP BY \"Company\" ORDER BY count(distinct \"ShipVia\") DESC")]
        public void GroupByAggregate_WithUnsupportedModifierOrColumnArg_IsRejected(string sql)
        {
            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountStar_AndGroupBySum_StillTranslate()
        {
            Assert.Equal("from 'Orders' group by Company select Company, count()",
                Translate("SELECT \"Company\", count(*) FROM \"Orders\" GROUP BY \"Company\""));

            Assert.Equal("from 'Orders' group by Company select Company, sum(Freight)",
                Translate("SELECT \"Company\", sum(\"Freight\") FROM \"Orders\" GROUP BY \"Company\""));
        }

        // count(1) counts every row, so it stays equivalent to count(*).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByCountOverConstant_StillTranslatesToCountStar()
        {
            Assert.Equal("from 'Orders' group by Company select Company, count()",
                Translate("SELECT \"Company\", count(1) FROM \"Orders\" GROUP BY \"Company\""));
        }

        // PG builds IS [NOT] DISTINCT FROM and NULLIF with a literal "=" operator name, so before the
        // A_Expr kind was checked they became a plain equality - the inverse of IS DISTINCT FROM.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT * FROM \"Orders\" WHERE \"Company\" IS DISTINCT FROM 'companies/85-A'")]
        [InlineData("SELECT * FROM \"Orders\" WHERE \"Company\" IS NOT DISTINCT FROM 'companies/85-A'")]
        [InlineData("SELECT * FROM \"Orders\" WHERE NULLIF(\"Company\", 'companies/85-A')")]
        public void DistinctFromPredicate_IsRejected(string sql)
        {
            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                sql, Array.Empty<int>(), out _));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void EqualityAndInequality_StillTranslate()
        {
            Assert.Equal("from 'Orders' where Company = 'companies/85-A'",
                Translate("SELECT * FROM \"Orders\" WHERE \"Company\" = 'companies/85-A'"));

            Assert.Equal("from 'Orders' where Company != 'companies/85-A'",
                Translate("SELECT * FROM \"Orders\" WHERE \"Company\" != 'companies/85-A'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKey_WithAlias_KeepsTheAlias()
        {
            Assert.Equal("from 'Orders' group by Company select Company as grp, count() as c",
                Translate("SELECT \"Company\" AS grp, COUNT(*) AS c FROM \"Orders\" GROUP BY \"Company\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKey_WithAlias_OrderByTheKeyStillResolves()
        {
            Assert.Equal("from 'Orders' group by Company order by Company desc select Company as grp, count() as c",
                Translate("SELECT \"Company\" AS grp, COUNT(*) AS c FROM \"Orders\" GROUP BY \"Company\" ORDER BY \"Company\" DESC"));
        }

        // An alias equal to the key adds nothing and would make RQL reject the duplicate alias.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKey_WithAliasEqualToTheKey_OmitsTheAsClause()
        {
            Assert.Equal("from 'Orders' group by Company select Company, count() as c",
                Translate("SELECT \"Company\" AS \"Company\", COUNT(*) AS c FROM \"Orders\" GROUP BY \"Company\""));
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("TO_TIMESTAMP('1996-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')", "'1996-07-01 00:00:00'::timestamp")]
        [InlineData("TO_TIMESTAMP('1996-07-01 13:45:30', 'YYYY-MM-DD HH24:MI:SS')", "'1996-07-01 13:45:30'::timestamp")]
        [InlineData("TO_TIMESTAMP('1996-07-01 13:45:30.123456', 'YYYY-MM-DD HH24:MI:SS.US')", "'1996-07-01 13:45:30.123456'::timestamp")]
        [InlineData("TO_TIMESTAMP('1996-07-01', 'YYYY-MM-DD')", "'1996-07-01'::timestamp")]
        [InlineData("TO_DATE('1996-07-01', 'YYYY-MM-DD')", "'1996-07-01'::timestamp")]
        public void TimestampFunctionBound_FoldsToTheSameValueAsTheCast(string bound, string cast)
        {
            Assert.Equal(
                Translate($"SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= {cast}"),
                Translate($"SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= {bound}"));
        }

        // PG's to_date discards the time component even when the format parses one.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void ToDateBound_DropsTheTimeComponent()
        {
            Assert.Equal(
                Translate("SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= '1996-07-01'::timestamp"),
                Translate("SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= TO_DATE('1996-07-01 13:45:30', 'YYYY-MM-DD HH24:MI:SS')"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void TimestampFunctionBound_TranslatesBothEndsOfAHalfOpenWindow()
        {
            var rql = Translate(
                """
                SELECT * FROM "Orders"
                WHERE "OrderedAt" >= TO_TIMESTAMP('1996-07-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                  AND "OrderedAt" < TO_TIMESTAMP('1996-10-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                """);

            Assert.Contains("1996-07-01", rql, StringComparison.Ordinal);
            Assert.Contains("1996-10-01", rql, StringComparison.Ordinal);
        }

        // A format token that isn't translated must fail the whole bound; passing it through as a
        // literal would silently shift the date.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("TO_TIMESTAMP('1996-Jul-01', 'YYYY-Mon-DD')")]
        [InlineData("TO_TIMESTAMP('1996-07-01', 'FMYYYY-FMMM-FMDD')")]
        [InlineData("TO_TIMESTAMP('01/07/1996 01:00 PM', 'DD/MM/YYYY HH12:MI AM')")]
        [InlineData("TO_DATE('1996 182', 'YYYY DDD')")]
        [InlineData("TO_TIMESTAMP('1996-07-01', '')")]
        public void TimestampFunctionBound_WithUnrecognisedFormat_IsRejected(string bound)
        {
            AssertRejected($"SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= {bound}");
        }

        // Neither argument can be resolved at translate time unless it is a string literal.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("TO_TIMESTAMP(\"RequireAt\", 'YYYY-MM-DD')")]
        [InlineData("TO_TIMESTAMP('1996-07-01', $1)")]
        [InlineData("TO_TIMESTAMP($1, 'YYYY-MM-DD')")]
        [InlineData("TO_TIMESTAMP(820454400)")]
        [InlineData("TO_TIMESTAMP('1996-07-01')")]
        public void TimestampFunctionBound_WithNonLiteralArgument_IsRejected(string bound)
        {
            AssertRejected($"SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= {bound}");
        }

        // The format is recognised but the value does not match it.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void TimestampFunctionBound_WithValueNotMatchingTheFormat_IsRejected()
        {
            AssertRejected("SELECT * FROM \"Orders\" WHERE \"OrderedAt\" >= TO_TIMESTAMP('not-a-date', 'YYYY-MM-DD')");
        }

        // to_timestamp is a WHERE-bound shape only.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT TO_TIMESTAMP('1996-07-01', 'YYYY-MM-DD') FROM \"Orders\"")]
        [InlineData("SELECT \"Company\", COUNT(*) FROM \"Orders\" GROUP BY TO_TIMESTAMP('1996-07-01', 'YYYY-MM-DD')")]
        public void TimestampFunction_OutsideAWhereBound_IsRejected(string sql)
        {
            AssertRejected(sql);
        }

        // The distinct-rows path (GROUP BY used as a DISTINCT, no aggregate) is a separate projection
        // path from the aggregate one and used to drop the SELECT alias.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKeyWithoutAggregate_KeepsTheAlias()
        {
            Assert.Equal("from 'Orders' select distinct Company as grp",
                Translate("SELECT \"Company\" AS grp FROM \"Orders\" GROUP BY \"Company\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKeysWithoutAggregate_KeepEachAlias()
        {
            Assert.Equal("from 'Orders' group by Company, Freight select Company as grp, Freight as f",
                Translate("SELECT \"Company\" AS grp, \"Freight\" AS f FROM \"Orders\" GROUP BY \"Company\", \"Freight\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKeyWithoutAggregate_NonIdentifierAliasIsQuoted()
        {
            Assert.Equal("from 'Orders' select distinct Company as 'the company'",
                Translate("SELECT \"Company\" AS \"the company\" FROM \"Orders\" GROUP BY \"Company\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupByKeyWithoutAggregate_AliasEqualToTheKeyOmitsTheAsClause()
        {
            Assert.Equal("from 'Orders' select distinct Company",
                Translate("SELECT \"Company\" AS \"Company\" FROM \"Orders\" GROUP BY \"Company\""));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderBySelectAlias_ResolvesToTheUnderlyingField()
        {
            Assert.Equal("from 'Orders' order by Freight select Freight as f",
                Translate("SELECT \"Freight\" AS f FROM \"Orders\" ORDER BY f"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByQuotedSelectAlias_ResolvesToTheUnderlyingField()
        {
            Assert.Equal("from 'Orders' order by Freight desc select Freight as 'the freight'",
                Translate("SELECT \"Freight\" AS \"the freight\" FROM \"Orders\" ORDER BY \"the freight\" DESC"));
        }

        // A sort key that is a real field rather than an alias keeps resolving as a field.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void OrderByRealFieldAlongsideAnAlias_IsUnchanged()
        {
            Assert.Equal("from 'Orders' order by Company select Freight as f",
                Translate("SELECT \"Freight\" AS f FROM \"Orders\" ORDER BY \"Company\""));
        }

        // An alias over a constant or id() has no document field behind it, so there is nothing to
        // sort on - reject instead of emitting a sort on a field that does not exist.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT 1 AS c0 FROM \"Orders\" ORDER BY c0")]
        [InlineData("SELECT \"id\" AS docid FROM \"Orders\" ORDER BY docid")]
        public void OrderByAliasOverANonField_IsRejected(string sql)
        {
            AssertRejected(sql);
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Freight\" FROM \"Orders\" ORDER BY \"Freight\" DESC NULLS LAST")]
        [InlineData("SELECT \"Freight\" FROM \"Orders\" ORDER BY \"Freight\" NULLS FIRST")]
        [InlineData("SELECT \"Freight\" FROM \"Orders\" ORDER BY \"Freight\" USING >")]
        [InlineData("SELECT \"Company\", COUNT(*) FROM \"Orders\" GROUP BY \"Company\" ORDER BY \"Company\" NULLS LAST")]
        public void OrderByWithAnUnsupportedSortModifier_IsRejected(string sql)
        {
            AssertRejected(sql);
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Freight\" FROM \"Orders\" ORDER BY \"Freight\" DESC", "from 'Orders' order by Freight desc select Freight")]
        [InlineData("SELECT \"Freight\" FROM \"Orders\" ORDER BY \"Freight\" ASC", "from 'Orders' order by Freight select Freight")]
        public void OrderByWithoutASortModifier_IsUnchanged(string sql, string expected)
        {
            Assert.Equal(expected, Translate(sql));
        }

        private static void AssertRejected(string sql)
        {
            Assert.False(Raven.Server.Integrations.PostgreSQL.Translation.PgSqlToRqlTranslator.TryParse(
                sql, Array.Empty<int>(), out _));
        }
    }
}
