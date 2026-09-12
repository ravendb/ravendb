using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.PostgreSqlHelper;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class PgDateBoundFilterTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const string SupersetFormat = "YYYY-MM-DD HH24:MI:SS";

        private const string Select = "SELECT \"Company\" FROM public.\"Orders\" WHERE ";

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Bound_before_the_range_keeps_every_order()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "A", "B", "C", "D", "E" },
                await Companies(Select + Bound(">=", "1996-01-01 00:00:00"), store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Bound_inside_the_range_filters()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "C", "D", "E" },
                await Companies(Select + Bound(">=", "1996-09-01 00:00:00"), store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Bound_after_the_range_keeps_nothing()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Empty(await Companies(Select + Bound(">=", "1999-01-01 00:00:00"), store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task Half_open_window_keeps_only_the_orders_inside_it()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "A", "B", "C" },
                await Companies(
                    Select + Bound(">=", "1996-07-01 00:00:00") + " AND " + Bound("<", "1996-10-01 00:00:00"),
                    store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task ToDate_bound_filters_on_the_date_alone()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            Assert.Equal(
                new[] { "C", "D", "E" },
                await Companies(
                    Select + "\"OrderedAt\" >= TO_DATE('1996-09-01', 'YYYY-MM-DD')", store, database));
        }

        [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
        public async Task ToTimestamp_bound_agrees_with_the_equivalent_cast()
        {
            using var store = GetDocumentStore();
            var database = await Seed(store);

            var viaFunction = await Companies(Select + Bound(">=", "1996-09-01 00:00:00"), store, database);
            var viaCast = await Companies(
                Select + "\"OrderedAt\" >= '1996-09-01 00:00:00'::timestamp", store, database);

            Assert.Equal(viaCast, viaFunction);
        }

        private static string Bound(string op, string value) =>
            $"\"OrderedAt\" {op} TO_TIMESTAMP('{value}', '{SupersetFormat}')";

        private async Task<DocumentDatabase> Seed(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Company = "A", OrderedAt = new DateTime(1996, 7, 4) });
                await session.StoreAsync(new Order { Company = "B", OrderedAt = new DateTime(1996, 8, 15) });
                await session.StoreAsync(new Order { Company = "C", OrderedAt = new DateTime(1996, 9, 20) });
                await session.StoreAsync(new Order { Company = "D", OrderedAt = new DateTime(1997, 1, 10) });
                await session.StoreAsync(new Order { Company = "E", OrderedAt = new DateTime(1998, 5, 6) });
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
            public DateTime OrderedAt { get; set; }
        }
    }
}
