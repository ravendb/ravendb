using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    /// <summary>
    /// RavenDB-27248: date/time fidelity on the PostgreSQL CDC streaming path.
    ///
    /// pgoutput delivers every column as a text literal, so the streaming path parses date/time
    /// values itself instead of getting typed values from Npgsql (unlike the initial load, which
    /// reads through a DbDataReader). These tests pin the two paths to the same representation.
    /// </summary>
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSink27248TimestampStreamingTests : CdcSinkIntegrationTestBase
    {
        public CdcSink27248TimestampStreamingTests(ITestOutputHelper output) : base(output)
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

        // Fields are read as strings so the assertions compare the stored JSON representation
        // rather than whatever the client deserializer would coerce it back into.
        private class Attr
        {
            public long DbId { get; set; }
            public string Name { get; set; }
            public string Ts { get; set; }
            public string TsTz { get; set; }
            public string Day { get; set; }
            public string Clock { get; set; }
        }

        private static CdcSinkConfiguration BuildConfig(string connectionStringName, string name) => new()
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
            var config = BuildConfig(sqlCs.Name, "test-27248-parity");
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
            var config = BuildConfig(sqlCs.Name, "test-27248-offset");
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
            var config = BuildConfig(sqlCs.Name, "test-27248-infinity");
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

        private class NopAttr
        {
            public long SourceId { get; set; }
            public long EntityId { get; set; }
            public string KeyGroup { get; set; }
            public string Value { get; set; }
            public string CreatedOrUpdatedDateUTC { get; set; }
        }

        /// <summary>
        /// Mirrors the nopCommerce GenericAttribute shape from the RavenDB-27248 report: quoted
        /// PascalCase identifiers, a TOASTed text column, and a nullable timestamp as the last
        /// column. Covers the timestamp on the initial-load row and on streamed INSERT/UPDATE of the
        /// same table, including an UPDATE that leaves the TOASTed column untouched, where pgoutput
        /// sends an "unchanged toasted value" placeholder.
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
                var loaded = await session.LoadAsync<NopAttr>("GenericAttributes/1");
                Assert.NotNull(loaded);
                Assert.Equal("2026-07-26T11:36:39.2766930", loaded.CreatedOrUpdatedDateUTC);
            }

            ExecuteNpgSql(connectionString, @"
                INSERT INTO ""GenericAttribute"" (""Id"", ""EntityId"", ""KeyGroup"", ""Key"", ""Value"", ""StoreId"", ""CreatedOrUpdatedDateUTC"")
                VALUES (2, 2, 'Product', 'Desc', repeat('y', 20000), 0, '2026-08-02 12:13:34.210777')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<NopAttr>("GenericAttributes/2");
                return a?.EntityId;
            }, 2L, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var streamed = await session.LoadAsync<NopAttr>("GenericAttributes/2");
                Assert.Equal("2026-08-02T12:13:34.2107770", streamed.CreatedOrUpdatedDateUTC);
            }

            // Streamed UPDATE that leaves the TOASTed Value untouched.
            ExecuteNpgSql(connectionString, @"
                UPDATE ""GenericAttribute"" SET ""EntityId"" = 22 WHERE ""Id"" = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var a = await session.LoadAsync<NopAttr>("GenericAttributes/2");
                return a?.EntityId;
            }, 22L, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var updated = await session.LoadAsync<NopAttr>("GenericAttributes/2");
                Assert.Equal("2026-08-02T12:13:34.2107770", updated.CreatedOrUpdatedDateUTC);
                Assert.Equal(20000, updated.Value?.Length ?? 0);
            }
        }
    }
}
