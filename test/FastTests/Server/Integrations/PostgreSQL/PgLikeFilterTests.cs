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
    // Superset emits LIKE / ILIKE for every text filter and for the search box, so the pattern shapes
    // below are the ones that decide whether its filters work at all.
    public sealed class PgLikeFilterTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string Select = "SELECT \"Company\" FROM public.\"Orders\" WHERE ";

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Like_prefix_is_case_sensitive()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Chocolade", "Chocolate Bar" },
                await Companies(Select + "\"Company\" LIKE 'Choc%'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Ilike_prefix_is_case_insensitive()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Chocolade", "Chocolate Bar", "chocolade" },
                await Companies(Select + "\"Company\" ILIKE 'choc%'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Like_suffix_is_case_sensitive()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Ernst Handel", "Handel" },
                await Companies(Select + "\"Company\" LIKE '%Handel'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Ilike_suffix_is_case_insensitive()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Ernst Handel", "Handel", "ernst handel" },
                await Companies(Select + "\"Company\" ILIKE '%handel'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Like_without_wildcards_is_case_sensitive_equality()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Chocolade" },
                await Companies(Select + "\"Company\" LIKE 'Chocolade'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Ilike_without_wildcards_is_case_insensitive_equality()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Chocolade", "chocolade" },
                await Companies(Select + "\"Company\" ILIKE 'chocolade'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Not_like_prefix()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Bon app'", "Ernst Handel", "Handel", "chocolade", "ernst handel" },
                await Companies(Select + "\"Company\" NOT LIKE 'Choc%'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Not_ilike_suffix()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Bon app'", "Chocolade", "Chocolate Bar", "chocolade" },
                await Companies(Select + "\"Company\" NOT ILIKE '%handel'", store, database));
        }

        // A document without the filtered field must not come back from a negated match, matching PG's
        // NULL handling rather than RQL's "everything the positive match didn't hit".
        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Not_like_excludes_documents_missing_the_field()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order());
                await session.SaveChangesAsync();
            }

            Assert.Equal(
                new[] { "Bon app'", "Ernst Handel", "Handel", "chocolade", "ernst handel" },
                await Companies(Select + "\"Company\" NOT LIKE 'Choc%'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Not_parenthesised_like_matches_not_like()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                await Companies(Select + "\"Company\" NOT LIKE 'Choc%'", store, database),
                await Companies(Select + "NOT (\"Company\" LIKE 'Choc%')", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Like_combines_with_other_predicates()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Chocolate Bar" },
                await Companies(Select + "\"Company\" LIKE 'Choc%' AND \"Freight\" > 1", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Like_pattern_with_quote_is_escaped()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "Bon app'" },
                await Companies(Select + "\"Company\" LIKE 'Bon app''%'", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Contains_pattern_is_rejected()
        {
            var message = await Rejected(Select + "\"Company\" LIKE '%choc%'");
            Assert.Contains("contains", message);
            Assert.Contains("search()", message);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Contains_pattern_is_rejected_for_ilike_too()
        {
            Assert.Contains("contains", await Rejected(Select + "\"Company\" ILIKE '%choc%'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Single_character_wildcard_is_rejected()
        {
            Assert.Contains("`_`", await Rejected(Select + "\"Company\" LIKE 'C_ocolade'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Interior_wildcard_is_rejected()
        {
            Assert.Contains("in the middle", await Rejected(Select + "\"Company\" LIKE 'Choc%ade'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Escaped_wildcard_is_rejected()
        {
            Assert.Contains("backslash escape", await Rejected(Select + "\"Company\" LIKE 'Choc\\%ade'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Wildcard_only_pattern_is_rejected()
        {
            Assert.Contains("matches every value", await Rejected(Select + "\"Company\" LIKE '%'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Escape_clause_is_rejected()
        {
            Assert.Contains("not a plain string literal", await Rejected(Select + "\"Company\" LIKE '!%choc%' ESCAPE '!'"));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Parameterized_pattern_is_rejected()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                Select + "\"Company\" LIKE $1", new[] { PgTypeOIDs.Text }, database, session: null));

            Assert.Contains("$n", error.Message);
        }

        private async Task<string> Rejected(string sql)
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var error = Assert.Throws<PgErrorException>(() => PgQuery.CreateInstance(
                sql, Array.Empty<int>(), database, session: null));

            return error.Message;
        }

        private async Task<DocumentDatabase> Seed(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Company = "Chocolade", Freight = 1 });
                await session.StoreAsync(new Order { Company = "chocolade", Freight = 1 });
                await session.StoreAsync(new Order { Company = "Chocolate Bar", Freight = 2 });
                await session.StoreAsync(new Order { Company = "Ernst Handel", Freight = 1 });
                await session.StoreAsync(new Order { Company = "ernst handel", Freight = 1 });
                await session.StoreAsync(new Order { Company = "Handel", Freight = 1 });
                await session.StoreAsync(new Order { Company = "Bon app'", Freight = 1 });
                await session.SaveChangesAsync();
            }

            return await Databases.GetDocumentDatabaseInstanceFor(store);
        }

        private async Task<string[]> Companies(string sql, IDocumentStore store, DocumentDatabase database)
        {
            await Run(sql, database);
            Indexes.WaitForIndexing(store);
            var rows = await Run(sql, database);
            return rows.Select(r => r[0]).OrderBy(x => x, StringComparer.Ordinal).ToArray();
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
