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

        // PowerBI's Top-N visual fires this shape: alias-qualified projection + alias-qualified
        // count argument, both needing the `rows.` fromAlias stripped to match the GROUP BY key.
        // The SQL alias on the aggregate must be preserved too: RQL's implicit alias for
        // count(Freight) would be `Freight`, colliding with the sibling group-by-key projection
        // (`Duplicate alias 'Freight'`), so we emit `count(Freight) as a0`.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void GroupBy_WithFromAlias_QualifiedProjectionAndAggregateArg_StripsAliasAndPreservesAggregateAlias()
        {
            var sql = """
                select "rows"."Freight" as "Freight", count("rows"."Freight") as "a0"
                from "public"."Orders" "rows"
                group by "Freight"
                limit 1000001
                """;
            var expected = "from 'Orders' group by Freight select Freight, count(Freight) as a0 limit 0, 1000001";

            Assert.Equal(expected, Translate(sql));
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
            var expected = "from 'orders' group by status order by 'count()' desc select status, count()";

            Assert.Equal(expected, Translate(sql));
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
    }
}
