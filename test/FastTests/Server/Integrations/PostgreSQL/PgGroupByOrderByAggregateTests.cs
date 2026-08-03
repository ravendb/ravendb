using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Tests.Infrastructure;
using Xunit;

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

            var token = TestContext.Current.CancellationToken;
            var pipe = new Pipe();
            var builder = new MessageBuilder();

            var readTask = ReadAllAsync(pipe.Reader, token);
            await query.Execute(builder, pipe.Writer, token);
            await pipe.Writer.CompleteAsync();
            var bytes = await readTask;

            return (columns.Select(c => c.Name).ToList(), ParseDataRows(bytes));
        }

        private static async Task<byte[]> ReadAllAsync(PipeReader reader, CancellationToken token)
        {
            var ms = new MemoryStream();
            while (true)
            {
                var result = await reader.ReadAsync(token);
                foreach (var segment in result.Buffer)
                    ms.Write(segment.Span);
                reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted)
                    break;
            }
            await reader.CompleteAsync();
            return ms.ToArray();
        }

        private static List<string[]> ParseDataRows(byte[] buffer)
        {
            var rows = new List<string[]>();
            int i = 0;
            while (i + 5 <= buffer.Length)
            {
                var type = buffer[i];
                int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(i + 1, 4));
                int payloadStart = i + 5;
                int payloadLength = length - 4;
                if (payloadLength < 0 || payloadStart + payloadLength > buffer.Length)
                    break;

                if (type == (byte)'D')
                {
                    int pos = payloadStart;
                    int count = BinaryPrimitives.ReadInt16BigEndian(buffer.AsSpan(pos, 2));
                    pos += 2;

                    var values = new string[count];
                    for (int c = 0; c < count; c++)
                    {
                        int size = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(pos, 4));
                        pos += 4;
                        if (size < 0)
                            continue;
                        values[c] = Encoding.UTF8.GetString(buffer, pos, size);
                        pos += size;
                    }

                    rows.Add(values);
                }

                i = payloadStart + payloadLength;
            }

            return rows;
        }

        private sealed class Order
        {
            public string Company { get; set; }
            public double Freight { get; set; }
        }
    }
}
