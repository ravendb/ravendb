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
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.PostgreSqlHelper;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // ORDER BY on the non-grouped path.
    public sealed class PgOrderByTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_a_select_alias_matches_ordering_by_the_field()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var viaAlias = await Freights(
                "SELECT \"Freight\" AS f FROM public.\"Orders\" ORDER BY f LIMIT 5", store, database);
            var viaField = await Freights(
                "SELECT \"Freight\" FROM public.\"Orders\" ORDER BY \"Freight\" LIMIT 5", store, database);

            Assert.Equal(new[] { "2", "10" }, viaField);
            Assert.Equal(viaField, viaAlias);
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_a_select_alias_descending_matches_ordering_by_the_field()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var viaAlias = await Freights(
                "SELECT \"Freight\" AS f FROM public.\"Orders\" ORDER BY f DESC LIMIT 5", store, database);

            Assert.Equal(new[] { "10", "2" }, viaAlias);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task OrderBy_an_alias_over_a_constant_returns_a_pg_error()
        {
            using var store = GetDocumentStore();
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                "SELECT 1 AS c0 FROM public.\"Orders\" ORDER BY c0", Array.Empty<int>(), database, session: null));

            Assert.False(string.IsNullOrWhiteSpace(error.Message));
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Freight\" FROM public.\"Orders\" ORDER BY \"Freight\" DESC NULLS LAST LIMIT 5", "NULLS")]
        [InlineData("SELECT \"Freight\" FROM public.\"Orders\" ORDER BY \"Freight\" USING > LIMIT 5", "USING")]
        public async Task OrderBy_with_an_unsupported_sort_modifier_returns_a_targeted_pg_error(string sql, string expected)
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                sql, Array.Empty<int>(), database, session: null));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, error.ErrorCode);
            Assert.Contains(expected, error.Message);
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task OrderBy_without_a_sort_modifier_is_unchanged()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "10", "2" },
                await Freights("SELECT \"Freight\" FROM public.\"Orders\" ORDER BY \"Freight\" DESC LIMIT 5", store, database));
        }

        private async Task<DocumentDatabase> Seed(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Company = "Alpha", Freight = 10 });
                await session.StoreAsync(new Order { Company = "Beta", Freight = 2 });
                await session.SaveChangesAsync();
            }

            return await Databases.GetDocumentDatabaseInstanceFor(store);
        }

        private async Task<string[]> Freights(string sql, IDocumentStore store, DocumentDatabase database)
        {
            await Run(sql, database);
            Indexes.WaitForIndexing(store);
            var rows = await Run(sql, database);
            return rows.Select(r => r[0]).ToArray();
        }

        private static async Task<List<string[]>> Run(string sql, DocumentDatabase database)
        {
            using var query = PgQuery.CreateInstance(sql, Array.Empty<int>(), database, session: null);
            await query.Init();

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
