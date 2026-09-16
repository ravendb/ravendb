using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.PostgreSqlHelper;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // "Top N by count" is the shape every BI tool emits for a bar chart. Superset names the
    // aggregate and sorts by the alias; PowerBI and friends repeat the aggregate function.
    public sealed class PgGroupByOrderByAggregateTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string SupersetQuery =
            "SELECT \"Company\" AS \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY count DESC LIMIT 10000";

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Superset_top_n_by_count_executes()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(SupersetQuery, store, database);

            Assert.Equal(new[] { "Company", "count" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]));
            Assert.Equal(new[] { "10", "9", "2" }, rows.Select(r => r[1]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_count_function_and_alias_agree()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var byFunction = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY COUNT(*) DESC", store, database);
            var byAlias = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY count DESC", store, database);

            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, byFunction.Rows.Select(r => r[0]));
            Assert.Equal(byFunction.Rows.Select(r => r[0]), byAlias.Rows.Select(r => r[0]));
            Assert.Equal(byFunction.Rows.Select(r => r[1]), byAlias.Rows.Select(r => r[1]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_count_ascending()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (_, rows) = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY COUNT(*) ASC", store, database);

            Assert.Equal(new[] { "Gamma", "Beta", "Alpha" }, rows.Select(r => r[0]));
            Assert.Equal(new[] { "2", "9", "10" }, rows.Select(r => r[1]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_sum_function_and_alias_agree()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var byFunction = await RunWarm(
                "SELECT \"Company\", SUM(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY SUM(\"Freight\") DESC", store, database);
            var byAlias = await RunWarm(
                "SELECT \"Company\", SUM(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY \"a0\" DESC", store, database);

            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, byFunction.Rows.Select(r => r[0]));
            Assert.Equal(byFunction.Rows.Select(r => r[1]), byAlias.Rows.Select(r => r[1]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_sum_ascending()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (_, rows) = await RunWarm(
                "SELECT \"Company\", SUM(\"Freight\") AS \"a0\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY SUM(\"Freight\") ASC", store, database);

            Assert.Equal(new[] { "Gamma", "Beta", "Alpha" }, rows.Select(r => r[0]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_group_key_still_works()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (_, rows) = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY \"Company\" DESC", store, database);

            Assert.Equal(new[] { "Gamma", "Beta", "Alpha" }, rows.Select(r => r[0]));
        }

        // Ordering by an aggregate the SELECT never emits has no RQL form; it must still be a
        // readable protocol error rather than an unhandled exception.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task OrderBy_aggregate_not_in_select_returns_a_pg_error()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT \"Company\", COUNT(*) FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY SUM(\"Freight\") DESC",
                Array.Empty<int>(), database, session: null));

            Assert.False(string.IsNullOrWhiteSpace(error.Message));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task ScalarCount_returns_the_collection_count()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using var query = PgQuery.CreateInstance(
                "SELECT COUNT(*) FROM public.\"Orders\"", Array.Empty<int>(), database, session: null);

            var column = Assert.Single(await query.Init());
            Assert.Equal("count", column.Name);
            Assert.Equal(PgTypeOIDs.Int8, column.PgType.Oid);

            Assert.Equal(new[] { "21" }, Assert.Single(await Drain(query)));
        }

        // Superset's exact shape: in SQL the LIMIT caps the aggregate's single output row, so it must
        // not become the engine's page size (which would return 0 rows, or a capped count).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task ScalarCount_with_a_sql_limit_is_not_capped()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await Run(
                "SELECT COUNT(*) AS \"COUNT(*)\" FROM public.\"Orders\" LIMIT 50000", database);

            Assert.Equal(new[] { "COUNT(*)" }, columns);
            Assert.Equal(new[] { "21" }, Assert.Single(rows));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task ScalarCount_honours_the_where_clause()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (_, rows) = await RunWarm(
                "SELECT COUNT(*) FROM public.\"Orders\" WHERE \"Company\" = 'Beta'", store, database);

            Assert.Equal(new[] { "9" }, Assert.Single(rows));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task ScalarSum_without_group_by_still_returns_a_pg_error()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT SUM(\"Freight\") FROM public.\"Orders\"", Array.Empty<int>(), database, session: null));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, error.ErrorCode);
            Assert.Contains("Scalar aggregate", error.Message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Count_mixed_with_a_bare_column_without_group_by_still_returns_a_pg_error()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT \"Company\", COUNT(*) FROM public.\"Orders\"", Array.Empty<int>(), database, session: null));

            Assert.Equal(PgErrorCodes.StatementTooComplex, error.ErrorCode);
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task GroupKey_alias_is_used_as_the_column_name()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\" AS grp, COUNT(*) AS c FROM public.\"Orders\" GROUP BY \"Company\"", store, database);

            Assert.Equal(new[] { "grp", "c" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]).OrderBy(x => x));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task GroupKey_alias_with_order_by_the_key_still_works()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\" AS grp, COUNT(*) AS c FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY \"Company\" DESC", store, database);

            Assert.Equal(new[] { "grp", "c" }, columns);
            Assert.Equal(new[] { "Gamma", "Beta", "Alpha" }, rows.Select(r => r[0]));
        }

        // These used to return the plain count(*)/sum() value for every group with no error at all.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Company\", count(distinct \"Freight\") FROM public.\"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(\"Freight\") FROM public.\"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", sum(distinct \"Freight\") FROM public.\"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(*) FILTER (WHERE \"Freight\" > 1) FROM public.\"Orders\" GROUP BY \"Company\"")]
        [InlineData("SELECT \"Company\", count(*) OVER () FROM public.\"Orders\" GROUP BY \"Company\"")]
        public async Task GroupByAggregate_withUnsupportedModifierOrColumnArg_returns_a_pg_error(string sql)
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                sql, Array.Empty<int>(), database, session: null));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, error.ErrorCode);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task IsDistinctFrom_returns_a_pg_error_instead_of_an_equality_match()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT count(*) FROM public.\"Orders\" WHERE \"Company\" IS DISTINCT FROM 'Alpha'",
                Array.Empty<int>(), database, session: null));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, error.ErrorCode);
            Assert.Contains("IS DISTINCT FROM", error.Message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task NotEquals_and_equals_still_count_correctly()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (_, notEqual) = await RunWarm(
                "SELECT COUNT(*) FROM public.\"Orders\" WHERE \"Company\" != 'Alpha'", store, database);
            var (_, equal) = await RunWarm(
                "SELECT COUNT(*) FROM public.\"Orders\" WHERE \"Company\" = 'Alpha'", store, database);

            Assert.Equal(new[] { "11" }, Assert.Single(notEqual));
            Assert.Equal(new[] { "10" }, Assert.Single(equal));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Aggregate_alias_that_is_not_an_identifier_is_kept()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS \"COUNT(*)\" FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY \"COUNT(*)\" DESC LIMIT 5",
                store, database);

            Assert.Equal(new[] { "Company", "COUNT(*)" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]));
            Assert.Equal(new[] { "10", "9", "2" }, rows.Select(r => r[1]));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Sum_alias_that_is_not_an_identifier_is_kept()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\", SUM(\"Freight\") AS \"SUM(Freight)\" FROM public.\"Orders\" GROUP BY \"Company\" LIMIT 5",
                store, database);

            Assert.Equal(new[] { "Company", "SUM(Freight)" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]).OrderBy(x => x));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Identifier_aggregate_alias_is_unchanged()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\", COUNT(*) AS total FROM public.\"Orders\" GROUP BY \"Company\" LIMIT 5", store, database);

            Assert.Equal(new[] { "Company", "total" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]).OrderBy(x => x));
        }

        // The distinct-rows path: a GROUP BY with no aggregate at all.
        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task GroupKey_alias_without_an_aggregate_is_used_as_the_column_name()
        {
            using var store = GetDocumentStore();
            await Seed(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var (columns, rows) = await RunWarm(
                "SELECT \"Company\" AS grp FROM public.\"Orders\" GROUP BY \"Company\" LIMIT 5", store, database);

            Assert.Equal(new[] { "grp" }, columns);
            Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, rows.Select(r => r[0]).OrderBy(x => x));
        }

        // An alias cannot be allowed to break out of the RQL string literal it is spliced into.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Aggregate_alias_with_a_quote_is_still_rejected()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT \"Company\", COUNT(*) AS \"c'x\" FROM public.\"Orders\" GROUP BY \"Company\"",
                Array.Empty<int>(), database, session: null));
        }

        // Alpha: 10 orders x 1.5 freight, Beta: 9 x 1, Gamma: 2 x 1. The counts (10/9/2) and the
        // sums (15/9/2) both order differently under string comparison than under numeric.
        private static async Task Seed(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();
            for (int i = 0; i < 10; i++)
                await session.StoreAsync(new Order { Company = "Alpha", Freight = 1.5 });
            for (int i = 0; i < 9; i++)
                await session.StoreAsync(new Order { Company = "Beta", Freight = 1 });
            for (int i = 0; i < 2; i++)
                await session.StoreAsync(new Order { Company = "Gamma", Freight = 1 });
            await session.SaveChangesAsync();
        }

        private async Task<(List<string> Columns, List<string[]> Rows)> RunWarm(string sql, IDocumentStore store, DocumentDatabase database)
        {
            await Run(sql, database);
            Indexes.WaitForIndexing(store);
            return await Run(sql, database);
        }

        private static async Task<(List<string> Columns, List<string[]> Rows)> Run(string sql, DocumentDatabase database)
        {
            using var query = PgQuery.CreateInstance(sql, Array.Empty<int>(), database, session: null);
            var columns = await query.Init();

            return (columns.Select(c => c.Name).ToList(), await Drain(query));
        }

        private static async Task<List<string[]>> Drain(PgQuery query)
        {
            var token = TestContext.Current.CancellationToken;
            var pipe = new Pipe();
            var builder = new MessageBuilder();

            var readTask = ReadAllAsync(pipe.Reader, token);
            await query.Execute(builder, pipe.Writer, token);
            await pipe.Writer.CompleteAsync();

            return ParseDataRows(await readTask);
        }

        private sealed class Order
        {
            public string Company { get; set; }
            public double Freight { get; set; }
        }
    }
}
