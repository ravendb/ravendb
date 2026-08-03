using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using SlowTests.Server.Documents.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    /// <summary>
    /// RavenDB-27248: date/time fidelity on the PostgreSQL CDC streaming path.
    ///
    /// pgoutput delivers every column as a text literal, so the streaming path parses date/time
    /// values itself instead of getting typed values from Npgsql (unlike the initial load, which
    /// reads through a DbDataReader). The first group of tests pins the two paths to the same
    /// representation.
    ///
    /// The second group reproduces the shape reported in the ticket against a real nopCommerce
    /// database; it is opt-in via RAVEN_NOPCOMMERCE_PG. Those tests document that the reported
    /// symptom (streamed timestamps arriving null) does not reproduce, and that an unmapped column
    /// is indistinguishable from a null one in the check the report used.
    /// </summary>
    [Collection(nameof(CdcSinkPostgresTests))]
    public class RavenDB_27248 : CdcSinkIntegrationTestBase
    {
        public RavenDB_27248(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, connectionString, sql);
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "pg-cdc-27248")
        {
            var sqlCs = new SqlConnectionString
            {
                Name = name,
                FactoryName = "Npgsql",
                ConnectionString = connectionString
            };

            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        // Date/time fields are read as strings so the assertions compare the stored JSON
        // representation rather than whatever the client deserializer would coerce it back into.
        private class Attr
        {
            public long DbId { get; set; }
            public string Name { get; set; }
            public string Ts { get; set; }
            public string TsTz { get; set; }
            public string Day { get; set; }
            public string Clock { get; set; }
        }

        private static CdcSinkConfiguration BuildAttrsConfig(string connectionStringName, string name) => new()
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            Tables = new List<CdcSinkTableConfig>
            {
                new CdcSinkTableConfig
                {
                    CollectionName = "Attrs",
                    SourceTableSchema = "public",
                    SourceTableName = "attrs",
                    PrimaryKeyColumns = new List<string> { "id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "id", Name = "DbId" },
                        new CdcColumnMapping { Column = "name", Name = "Name" },
                        new CdcColumnMapping { Column = "ts", Name = "Ts" },
                        new CdcColumnMapping { Column = "ts_tz", Name = "TsTz" },
                        new CdcColumnMapping { Column = "day", Name = "Day" },
                        new CdcColumnMapping { Column = "clock", Name = "Clock" }
                    }
                }
            }
        };

        private const string CreateAttrsTable = @"
            CREATE TABLE attrs (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                ts TIMESTAMP NULL,
                ts_tz TIMESTAMPTZ NULL,
                day DATE NULL,
                clock TIME NULL
            )";

        /// <summary>
        /// A streamed row must store the same date/time representation as the same values read by
        /// the initial load. Before the fix, timestamptz was re-anchored to the RavenDB host's local
        /// time zone and stored without a zone marker, so every streamed row's instant was shifted.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task DateTimeTypes_StreamedRow_MatchesInitialLoad()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, CreateAttrsTable);

            ExecuteNpgSql(connectionString, @"
                INSERT INTO attrs (id, name, ts, ts_tz, day, clock)
                VALUES (1, 'initial', '2026-08-02 12:13:34.210777', '2026-08-02 12:13:34.210777+00', '2026-08-02', '12:13:34.567')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildAttrsConfig(sqlCs.Name, "test-27248-parity");
            AddCdcSink(store, config);

            await WaitForCdcInitialLoadAsync(store, config.Name);
            await WaitForDocumentCountAsync(store, "Attrs", expectedCount: 1, timeoutMs: 60_000);

            Attr initial;
            using (var session = store.OpenAsyncSession())
            {
                initial = await session.LoadAsync<Attr>("Attrs/1");
            }

            Assert.NotNull(initial);
            Assert.Equal("2026-08-02T12:13:34.2107770", initial.Ts);
            Assert.Equal("2026-08-02T12:13:34.2107770Z", initial.TsTz);
            Assert.Equal("2026-08-02", initial.Day);

            // Same values, delivered over the replication stream instead.
            ExecuteNpgSql(connectionString, @"
                INSERT INTO attrs (id, name, ts, ts_tz, day, clock)
                VALUES (2, 'streamed', '2026-08-02 12:13:34.210777', '2026-08-02 12:13:34.210777+00', '2026-08-02', '12:13:34.567')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<Attr>("Attrs/2");
                return a?.Name;
            }, "streamed", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var streamed = await session.LoadAsync<Attr>("Attrs/2");

                Assert.Equal(initial.Ts, streamed.Ts);
                Assert.Equal(initial.TsTz, streamed.TsTz);
                Assert.Equal(initial.Day, streamed.Day);
                Assert.Equal(initial.Clock, streamed.Clock);
            }
        }

        /// <summary>
        /// A timestamptz written from a non-UTC offset denotes the same instant as its UTC form, so
        /// the streamed document must store that instant in UTC, not the offset's wall-clock time.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task TimestampTz_StreamedWithNonUtcOffset_StoredAsUtcInstant()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, CreateAttrsTable);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildAttrsConfig(sqlCs.Name, "test-27248-offset");
            AddCdcSink(store, config);

            await WaitForCdcInitialLoadAsync(store, config.Name);

            // 09:30:00-05:00 is the instant 14:30:00Z.
            ExecuteNpgSql(connectionString, @"
                INSERT INTO attrs (id, name, ts, ts_tz, day)
                VALUES (1, 'offset', NULL, '2026-08-02 09:30:00-05:00', NULL)");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<Attr>("Attrs/1");
                return a?.Name;
            }, "offset", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var streamed = await session.LoadAsync<Attr>("Attrs/1");

                Assert.Equal("2026-08-02T14:30:00.0000000Z", streamed.TsTz);
                Assert.Null(streamed.Ts);
                Assert.Null(streamed.Day);
            }
        }

        /// <summary>
        /// Postgres accepts "infinity"/"-infinity" for timestamp, timestamptz and date. The
        /// initial-load reader turns those into DateTime/DateOnly Max/Min; the streaming path used to
        /// hit a FormatException on the text literal, which fails the batch and stalls the stream.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task DateTimeTypes_Infinity_StreamsWithoutStallingTheTask()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, CreateAttrsTable);

            ExecuteNpgSql(connectionString, @"
                INSERT INTO attrs (id, name, ts, ts_tz, day)
                VALUES (1, 'initial', 'infinity', 'infinity', 'infinity')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildAttrsConfig(sqlCs.Name, "test-27248-infinity");
            AddCdcSink(store, config);

            await WaitForCdcInitialLoadAsync(store, config.Name);
            await WaitForDocumentCountAsync(store, "Attrs", expectedCount: 1, timeoutMs: 60_000);

            Attr initial;
            using (var session = store.OpenAsyncSession())
            {
                initial = await session.LoadAsync<Attr>("Attrs/1");
            }

            ExecuteNpgSql(connectionString, @"
                INSERT INTO attrs (id, name, ts, ts_tz, day)
                VALUES (2, 'streamed-pos', 'infinity', 'infinity', 'infinity');
                INSERT INTO attrs (id, name, ts, ts_tz, day)
                VALUES (3, 'streamed-neg', '-infinity', '-infinity', '-infinity');");

            // Row 3 arriving proves the stream did not stall on row 2.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<Attr>("Attrs/3");
                return a?.Name;
            }, "streamed-neg", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var positive = await session.LoadAsync<Attr>("Attrs/2");
                Assert.Equal(initial.Ts, positive.Ts);
                Assert.Equal(initial.TsTz, positive.TsTz);
                Assert.Equal(initial.Day, positive.Day);

                var negative = await session.LoadAsync<Attr>("Attrs/3");
                Assert.NotNull(negative.Ts);
                Assert.NotNull(negative.TsTz);
                Assert.NotNull(negative.Day);
                Assert.NotEqual(positive.Ts, negative.Ts);
            }
        }

        private class GenericAttr
        {
            public long SourceId { get; set; }
            public long EntityId { get; set; }
            public string KeyGroup { get; set; }
            public string Key { get; set; }
            public string Value { get; set; }
            public string CreatedOrUpdatedDateUTC { get; set; }
        }

        /// <summary>
        /// Mirrors the nopCommerce GenericAttribute shape from the report: quoted PascalCase
        /// identifiers, a TOASTed text column, and a nullable timestamp as the last column. Covers
        /// the timestamp on the initial-load row and on streamed INSERT/UPDATE of the same table,
        /// including an UPDATE that leaves the TOASTed column untouched, where pgoutput sends an
        /// "unchanged toasted value" placeholder.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task Timestamp_NopCommerceShape_SurvivesStreaming()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE ""GenericAttribute"" (
                    ""Id"" INTEGER NOT NULL PRIMARY KEY,
                    ""EntityId"" INTEGER NOT NULL,
                    ""KeyGroup"" VARCHAR(400) NOT NULL,
                    ""Key"" VARCHAR(400) NOT NULL,
                    ""Value"" TEXT NOT NULL,
                    ""StoreId"" INTEGER NOT NULL,
                    ""CreatedOrUpdatedDateUTC"" TIMESTAMP NULL
                )");

            // Long Value so the column is TOASTed out of line.
            ExecuteNpgSql(connectionString, @"
                INSERT INTO ""GenericAttribute"" (""Id"", ""EntityId"", ""KeyGroup"", ""Key"", ""Value"", ""StoreId"", ""CreatedOrUpdatedDateUTC"")
                VALUES (1, 1, 'Product', 'Desc', repeat('x', 20000), 0, '2026-07-26 11:36:39.276693')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-27248-nop",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "GenericAttributes",
                        SourceTableSchema = "public",
                        SourceTableName = "GenericAttribute",
                        PrimaryKeyColumns = new List<string> { "Id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "Id", Name = "SourceId" },
                            new CdcColumnMapping { Column = "EntityId", Name = "EntityId" },
                            new CdcColumnMapping { Column = "KeyGroup", Name = "KeyGroup" },
                            new CdcColumnMapping { Column = "Value", Name = "Value" },
                            new CdcColumnMapping { Column = "CreatedOrUpdatedDateUTC", Name = "CreatedOrUpdatedDateUTC" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await WaitForCdcInitialLoadAsync(store, config.Name);
            await WaitForDocumentCountAsync(store, "GenericAttributes", expectedCount: 1, timeoutMs: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<GenericAttr>("GenericAttributes/1");
                Assert.NotNull(loaded);
                Assert.Equal("2026-07-26T11:36:39.2766930", loaded.CreatedOrUpdatedDateUTC);
            }

            ExecuteNpgSql(connectionString, @"
                INSERT INTO ""GenericAttribute"" (""Id"", ""EntityId"", ""KeyGroup"", ""Key"", ""Value"", ""StoreId"", ""CreatedOrUpdatedDateUTC"")
                VALUES (2, 2, 'Product', 'Desc', repeat('y', 20000), 0, '2026-08-02 12:13:34.210777')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<GenericAttr>("GenericAttributes/2");
                return a?.EntityId;
            }, 2L, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var streamed = await session.LoadAsync<GenericAttr>("GenericAttributes/2");
                Assert.Equal("2026-08-02T12:13:34.2107770", streamed.CreatedOrUpdatedDateUTC);
            }

            // Streamed UPDATE that leaves the TOASTed Value untouched.
            ExecuteNpgSql(connectionString, @"
                UPDATE ""GenericAttribute"" SET ""EntityId"" = 22 WHERE ""Id"" = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<GenericAttr>("GenericAttributes/2");
                return a?.EntityId;
            }, 22L, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var updated = await session.LoadAsync<GenericAttr>("GenericAttributes/2");
                Assert.Equal("2026-08-02T12:13:34.2107770", updated.CreatedOrUpdatedDateUTC);
                Assert.Equal(20000, updated.Value?.Length ?? 0);
            }
        }

        // ---------------------------------------------------------------------------------------
        // Reproduction against a real nopCommerce 4.90.6 PostgreSQL database, using the reporter's
        // own writer SQL and the same concurrency + restart shape.
        //
        // These mutate a live nopCommerce database and restart its container, so they are opt-in:
        // they run only when RAVEN_NOPCOMMERCE_PG points at a disposable instance, and skip
        // (rather than fail) everywhere else, CI included.
        // ---------------------------------------------------------------------------------------

        private const string ConnectionStringEnvName = "RAVEN_NOPCOMMERCE_PG";
        private const string ContainerEnvName = "RAVEN_NOPCOMMERCE_PG_CONTAINER";

        private static readonly string NopConnectionString =
            Environment.GetEnvironmentVariable(ConnectionStringEnvName);

        private static readonly string NopContainer =
            Environment.GetEnvironmentVariable(ContainerEnvName) ?? "nop-postgres";

        private const string Publication = "cdc_27248_pub";
        private const string Slot = "cdc_27248_slot";
        private const int RangeStart = 40000;

        private static void SkipUnlessConfigured()
        {
            Assert.SkipWhen(string.IsNullOrEmpty(NopConnectionString),
                $"Set {ConnectionStringEnvName} to a disposable nopCommerce PostgreSQL instance to run this test. " +
                $"It deletes and inserts \"GenericAttribute\" rows and restarts the '{NopContainer}' container.");
        }

        // The source container is restarted mid-test, so every statement has to ride out a restart.
        private static TResult WithRetry<TResult>(Func<NpgsqlCommand, TResult> body, string sql)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    using var conn = new NpgsqlConnection(NopConnectionString);
                    conn.Open();
                    using var cmd = new NpgsqlCommand(sql, conn);
                    cmd.CommandTimeout = 120;
                    return body(cmd);
                }
                catch (Exception e) when (attempt < 90 && IsTransient(e))
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static bool IsTransient(Exception e) => e switch
        {
            // 57P03 starting up, 57P01 admin shutdown, 08006/08003 connection failure
            PostgresException pe => pe.SqlState is "57P03" or "57P01" or "08006" or "08003",
            NpgsqlException => true,
            _ => false,
        };

        private static void Exec(string sql) => WithRetry(cmd => cmd.ExecuteNonQuery(), sql);

        private static T Scalar<T>(string sql) => WithRetry(cmd =>
        {
            var result = cmd.ExecuteScalar();
            return result is null or DBNull ? default : (T)Convert.ChangeType(result, typeof(T));
        }, sql);

        private static void CleanupReplication()
        {
            // A slot still held by a previous run's walsender cannot be dropped; kick it off first.
            for (int attempt = 0; attempt < 15; attempt++)
            {
                try
                {
                    Exec($@"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                            WHERE slot_name='{Slot}' AND active_pid IS NOT NULL");
                    Exec($"SELECT pg_drop_replication_slot('{Slot}') FROM pg_replication_slots WHERE slot_name='{Slot}'");
                    break;
                }
                catch (PostgresException e) when (e.SqlState == "55006") // object_in_use
                {
                    Thread.Sleep(1000);
                }
            }

            Exec($"DROP PUBLICATION IF EXISTS {Publication}");
        }

        // The reporter's writer, one batch.
        private static void WriteBatch(int lo, int hi) => Exec($@"
            INSERT INTO ""GenericAttribute"" (""Id"",""KeyGroup"",""Key"",""Value"",""EntityId"",""StoreId"",""CreatedOrUpdatedDateUTC"")
            SELECT s,'CdcLoad','k'||s,'v'||s,s,1,now() FROM generate_series({lo},{hi}) s
            ON CONFLICT (""Id"") DO NOTHING;");

        private static CdcSinkConfiguration BuildNopConfig(string connectionStringName) => new()
        {
            Name = "nop-27248",
            ConnectionStringName = connectionStringName,
            Postgres = new CdcSinkPostgresSettings
            {
                PublicationName = Publication,
                SlotName = Slot
            },
            Tables = new List<CdcSinkTableConfig>
            {
                new CdcSinkTableConfig
                {
                    CollectionName = "GenericAttributes",
                    SourceTableSchema = "public",
                    SourceTableName = "GenericAttribute",
                    PrimaryKeyColumns = new List<string> { "Id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new CdcColumnMapping { Column = "Id", Name = "SourceId" },
                        new CdcColumnMapping { Column = "EntityId", Name = "EntityId" },
                        new CdcColumnMapping { Column = "KeyGroup", Name = "KeyGroup" },
                        new CdcColumnMapping { Column = "Key", Name = "Key" },
                        new CdcColumnMapping { Column = "Value", Name = "Value" },
                        new CdcColumnMapping { Column = "StoreId", Name = "StoreId" },
                        new CdcColumnMapping { Column = "CreatedOrUpdatedDateUTC", Name = "CreatedOrUpdatedDateUTC" }
                    }
                }
            }
        };

        private SqlConnectionString SetupNopConnectionString(IDocumentStore store)
        {
            var sqlCs = new SqlConnectionString
            {
                Name = "nop-pg",
                FactoryName = "Npgsql",
                ConnectionString = NopConnectionString
            };

            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task NopCommerce_StreamedInserts_KeepTimestamp()
        {
            SkipUnlessConfigured();

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var config = BuildNopConfig(SetupNopConnectionString(store).Name);

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);

                WriteBatch(RangeStart, RangeStart + 249);
                WriteBatch(RangeStart + 250, RangeStart + 499);

                var lastId = RangeStart + 499;
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    var a = await session.LoadAsync<GenericAttr>($"GenericAttributes/{lastId}");
                    return a?.EntityId;
                }, (long)lastId, timeout: 120_000);

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var id in new[] { RangeStart, RangeStart + 123, RangeStart + 250, lastId })
                    {
                        var doc = await session.LoadAsync<GenericAttr>($"GenericAttributes/{id}");
                        Assert.NotNull(doc);
                        var sourceTs = Scalar<DateTime>(
                            $"SELECT \"CreatedOrUpdatedDateUTC\" FROM \"GenericAttribute\" WHERE \"Id\"={id}");

                        Assert.NotNull(doc.CreatedOrUpdatedDateUTC);
                        Assert.Equal(sourceTs.ToString("yyyy-MM-ddTHH:mm:ss.fffffff"), doc.CreatedOrUpdatedDateUTC);
                    }
                }
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }

        /// <summary>
        /// The ticket's actual repro: three concurrent writers, the sink restarted mid-stream, and
        /// the source PostgreSQL restarted while writes are still in flight. Asserts every streamed
        /// row landed with a timestamp matching the source.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task NopCommerce_ConcurrentWriters_WithSinkAndSourceRestart_KeepTimestamp()
        {
            SkipUnlessConfigured();

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var config = BuildNopConfig(SetupNopConnectionString(store).Name);

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);

                // The ticket's exact three ranges: 60000 rows total.
                var ranges = new[] { (40000, 49999), (50000, 79999), (80000, 99999) };

                var written = 0;
                var writers = new List<Task>();
                foreach (var (lo, hi) in ranges)
                {
                    var l = lo;
                    var h = hi;
                    writers.Add(Task.Run(() =>
                    {
                        for (int g = l; g <= h; g += 250)
                        {
                            var batchHi = Math.Min(g + 249, h);
                            WriteBatch(g, batchHi); // Exec already rides out a source restart
                            Interlocked.Add(ref written, batchHi - g + 1);
                            Thread.Sleep(300);
                        }
                    }));
                }

                // Drive the restarts off writer progress so both land while writes are in flight,
                // regardless of how fast this machine is.
                var sw = Stopwatch.StartNew();
                await WaitForWrittenAsync(() => written, 9_000, writers);
                Output.WriteLine($"[{sw.Elapsed}] restarting sink at written={written}");
                await RestartSinkAsync(store, config);

                await WaitForWrittenAsync(() => written, 25_000, writers);
                Output.WriteLine($"[{sw.Elapsed}] restarting source at written={written}");
                RestartSourceContainer();
                Output.WriteLine($"[{sw.Elapsed}] source back at written={written}");

                Assert.True(written < 60_000, $"restarts must overlap the writes, but all rows were already written (written={written})");

                await Task.WhenAll(writers);
                Output.WriteLine($"[{sw.Elapsed}] writers done");

                var srcRange = Scalar<long>($@"SELECT count(*) FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
                var srcTotal = Scalar<long>(@"SELECT count(*) FROM ""GenericAttribute""");
                Assert.Equal(60_000, srcRange);

                // Wait for the mirror to catch up to the source row count.
                var ravenCount = await WaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    return await session.Query<GenericAttr>(collectionName: "GenericAttributes").CountAsync();
                }, (int)srcTotal, timeout: 300_000, interval: 2000);

                Output.WriteLine($"source total={srcTotal} range={srcRange} raven={ravenCount}");

                // Compare EVERY streamed row's timestamp against the source, in one pass.
                var srcTimestamps = new Dictionary<long, DateTime?>();
                WithRetry(cmd =>
                {
                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                        srcTimestamps[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                    return 0;
                }, $@"SELECT ""Id"", ""CreatedOrUpdatedDateUTC"" FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");

                var nulls = new List<long>();
                var mismatches = new List<string>();
                var missing = new List<long>();

                foreach (var chunk in srcTimestamps.Keys.OrderBy(k => k).Chunk(1024))
                {
                    using var session = store.OpenAsyncSession();
                    var ids = chunk.Select(id => $"GenericAttributes/{id}").ToArray();
                    var docs = await session.LoadAsync<GenericAttr>(ids);

                    foreach (var id in chunk)
                    {
                        if (docs.TryGetValue($"GenericAttributes/{id}", out var doc) == false || doc is null)
                        {
                            missing.Add(id);
                            continue;
                        }

                        if (doc.CreatedOrUpdatedDateUTC is null)
                        {
                            nulls.Add(id);
                            continue;
                        }

                        var expected = srcTimestamps[id]?.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                        if (expected != doc.CreatedOrUpdatedDateUTC && mismatches.Count < 20)
                            mismatches.Add($"{id}: source={expected} raven={doc.CreatedOrUpdatedDateUTC}");
                    }
                }

                Output.WriteLine($"missing={missing.Count} nullTimestamps={nulls.Count} mismatches={mismatches.Count}");
                if (nulls.Count > 0)
                    Output.WriteLine("first null ids: " + string.Join(", ", nulls.Take(20)));
                if (missing.Count > 0)
                    Output.WriteLine("first missing ids: " + string.Join(", ", missing.Take(20)));
                foreach (var m in mismatches)
                    Output.WriteLine(m);

                Assert.Empty(nulls);
                Assert.Empty(mismatches);
                Assert.Empty(missing);
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }

        /// <summary>
        /// Demonstrates the alternative reading of the ticket's evidence: when
        /// CreatedOrUpdatedDateUTC is absent from the column mapping, the document simply has no such
        /// property. The reporter's check (jq '.CreatedOrUpdatedDateUTC') prints null for a missing
        /// key exactly as it does for a null value, so the two are indistinguishable in that output.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task NopCommerce_UnmappedTimestampColumn_LooksIdenticalToANullValue()
        {
            SkipUnlessConfigured();

            Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            CleanupReplication();

            using var store = GetDocumentStore();
            var config = BuildNopConfig(SetupNopConnectionString(store).Name);
            // Drop exactly the one column, leaving everything else as the passing test has it.
            config.Tables[0].Columns.RemoveAll(c => c.Column == "CreatedOrUpdatedDateUTC");

            try
            {
                AddCdcSink(store, config);
                await WaitForCdcInitialLoadAsync(store, config.Name, timeoutMs: 120_000);

                WriteBatch(RangeStart, RangeStart + 9);

                await AssertWaitForValueAsync(async () =>
                {
                    using var session = store.OpenAsyncSession();
                    var a = await session.LoadAsync<GenericAttr>($"GenericAttributes/{RangeStart}");
                    return a?.EntityId;
                }, (long)RangeStart, timeout: 120_000);

                // The source row definitely has a timestamp.
                var sourceTs = Scalar<DateTime>(
                    $"SELECT \"CreatedOrUpdatedDateUTC\" FROM \"GenericAttribute\" WHERE \"Id\"={RangeStart}");
                Assert.NotEqual(default, sourceTs);

                // Fetch the raw document over HTTP, the same way the ticket's repro did with curl.
                using (var http = new System.Net.Http.HttpClient())
                {
                    var json = await http.GetStringAsync(
                        $"{store.Urls[0]}/databases/{store.Database}/docs?id=GenericAttributes/{RangeStart}");
                    Output.WriteLine("document JSON: " + json);
                    Assert.DoesNotContain("CreatedOrUpdatedDateUTC", json);
                }
            }
            finally
            {
                CleanupReplication();
                Exec($@"DELETE FROM ""GenericAttribute"" WHERE ""Id"" >= {RangeStart}");
            }
        }

        private static async Task WaitForWrittenAsync(Func<int> written, int target, List<Task> writers)
        {
            while (written() < target)
            {
                if (writers.All(w => w.IsCompleted))
                    return;
                await Task.Delay(100);
            }
        }

        private async Task RestartSinkAsync(IDocumentStore store, CdcSinkConfiguration config)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.First(p => p.Name == config.Name);
            process.Stop("RavenDB-27248 repro restart");
            await Task.Delay(TimeSpan.FromSeconds(2));
            process.Start();
        }

        private static void RestartSourceContainer()
        {
            var psi = new System.Diagnostics.ProcessStartInfo("docker", $"restart {NopContainer}")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            using var p = System.Diagnostics.Process.Start(psi);
            p.WaitForExit(120_000);
        }
    }
}
