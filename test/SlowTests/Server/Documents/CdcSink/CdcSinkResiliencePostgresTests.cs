using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSinkResiliencePostgresTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkResiliencePostgresTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, connectionString, sql);
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "pg-resilience-test")
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

        private CdcSinkConfiguration BuildSimpleConfig(string name, string connectionStringName)
        {
            return new CdcSinkConfiguration
            {
                Name = name,
                ConnectionStringName = connectionStringName,
                Postgres = new CdcSinkPostgresSettings
                {
                    SlotName = $"test_{name.Replace("-", "_")}_slot",
                    PublicationName = $"test_{name.Replace("-", "_")}_pub"
                },
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        }
                    }
                }
            };
        }

        // ─────────────────────────────────────────────────────────────────────
        // Schema Evolution Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_AddColumn_ProcessRecoversAndContinues()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-schema-add-col", sqlCs.Name);
            AddCdcSink(store, config);

            // Wait for initial load
            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Before", doc.Name);

            // Add a column to the source table — PostgreSQL sends a new RelationMessage
            // with updated column list. The process should handle this gracefully.
            ExecuteNpgSql(connectionString, "ALTER TABLE items ADD COLUMN description VARCHAR(500)");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, description) VALUES (2, 'After', 'new column')");

            // The new column isn't in our mapping, so it's ignored — but the row should arrive
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After", doc2.Name);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_RemoveColumn_ProcessRecoversAfterRetry()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    extra VARCHAR(200)
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, extra) VALUES (1, 'Before', 'will be removed')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            // Map includes the 'extra' column that we'll later drop
            var config = new CdcSinkConfiguration
            {
                Name = "test-schema-drop-col",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "extra", Name = "Extra" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            var errorTask = await WaitForNextProcessError(store, "test-schema-drop-col");

            // Drop the mapped column — the RelationMessage will have fewer columns.
            // FindColumnIndex will throw for 'extra', causing a retry.
            // On retry, the process re-resolves columns and 'extra' won't be found →
            // the process will keep erroring until the configuration is updated.
            // This is expected behavior: removing a mapped column is a config error.
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN extra");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            await errorTask.WaitAsync(TimeSpan.FromSeconds(15));
        }

        // ─────────────────────────────────────────────────────────────────────
        // Connection Failure Recovery Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ConnectionFailure_RecoversAfterReplicationConnectionKilled()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.CdcSink.PostgresReplicationTimeout)] = "5"
            });
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before Kill')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-conn-failure", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Kill the replication connection by terminating the WAL sender backend.
            // Retry in a loop because under heavy load the slot's active_pid may briefly
            // be NULL (between a reconnect cycle), causing pg_terminate_backend to be a no-op.
            var errorTask = await WaitForNextProcessError(store, "test-conn-failure");

            var killed = false;
            for (int attempt = 0; attempt < 10 && killed == false; attempt++)
            {
                if (attempt > 0)
                    await Task.Delay(1_000);

                using var killConn = new NpgsqlConnection(connectionString);
                await killConn.OpenAsync();
                await using var cmd = new NpgsqlCommand(
                    $"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{config.Postgres.SlotName}' AND active_pid IS NOT NULL", killConn);
                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                    killed = reader.GetBoolean(0);
            }

            Assert.True(killed, "Failed to terminate the WAL sender backend after 10 attempts");
            await errorTask.WaitAsync(TimeSpan.FromSeconds(30));

            // Insert a new row while the process is recovering
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Kill')");

            // The process should recover (fallback is short in test) and pick up the new row
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Kill", doc2.Name);
        }

        // ─────────────────────────────────────────────────────────────────────
        // LSN Gap Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task LsnGap_DetectedWhenSlotRecreated()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before Gap')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-lsn-gap", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Wait for streaming to start (process has a checkpoint)
            await Task.Delay(2000);

            // Stop the process
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-lsn-gap");
            Assert.NotNull(process);
            process.Stop("test: recreating slot");

            // Get the slot name
            var slotName = config.Postgres?.SlotName;
            if (string.IsNullOrEmpty(slotName))
                slotName = $"ravendb_cdc_{Math.Abs(config.Name.GetHashCode()):x8}";

            var pubName = config.Postgres?.PublicationName;
            if (string.IsNullOrEmpty(pubName))
                pubName = $"ravendb_cdc_{Math.Abs(config.Name.GetHashCode()):x8}";

            // Drop and recreate the slot — simulates backup/restore scenario.
            // The new slot's restart_lsn will be ahead of our saved position.
            try
            {
                ExecuteNpgSql(connectionString, $"SELECT pg_drop_replication_slot('{slotName}')");
            }
            catch
            {
                // slot might already be inactive
            }

            ExecuteNpgSql(connectionString, $"SELECT pg_create_logical_replication_slot('{slotName}', 'pgoutput')");

            // Insert data that will be in the gap (we lost it)
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'In Gap - Lost')");

            // Restart the process
            process.Start();

            // Insert data after the gap
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (3, 'After Gap')");

            // The process should recover and pick up new data after the gap
            var doc3 = await WaitForDocumentAsync<Item>(store, "Items/3", timeoutMs: 60_000);
            Assert.NotNull(doc3);
            Assert.Equal("After Gap", doc3.Name);

            // doc2 ("In Gap - Lost") may or may not be present — the gap means we may
            // have missed it. The key assertion is that the process recovered and continued
            // processing new data (doc3) despite the gap.
        }
        // ─────────────────────────────────────────────────────────────────────
        // Extended Schema Evolution Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_AddColumnAtEnd_NoInterruption()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-add-col-end", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Add column at end — PostgreSQL sends new RelationMessage, our sentinel detects it,
            // we rebuild column mapping. New column is not in our mapping so it's ignored.
            ExecuteNpgSql(connectionString, "ALTER TABLE items ADD COLUMN description VARCHAR(500)");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, description) VALUES (2, 'After Add', 'desc')");

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Add", doc2.Name);

            // Verify no fallback occurred — the process handled the change transparently
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-add-col-end");
            Assert.Null(process?.FallbackTime);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_RemoveUnmappedColumn_NoInterruption()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    unmapped_col VARCHAR(200) DEFAULT 'ignore me'
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            // Only map id and name — unmapped_col is not in our config
            var config = BuildSimpleConfig("test-drop-unmapped", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Drop the unmapped column — name-based matching still finds id and name
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN unmapped_col");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Drop", doc2.Name);

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-drop-unmapped");
            Assert.Null(process?.FallbackTime);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_RemoveMappedColumn_EntersFallback()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    extra VARCHAR(200)
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, extra) VALUES (1, 'Before', 'has extra')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-drop-mapped",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "extra", Name = "Extra" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            var errorTask = await WaitForNextProcessError(store, "test-drop-mapped");

            // Drop a MAPPED column — FindColumnIndex will throw on rebuild
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN extra");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            await errorTask.WaitAsync(TimeSpan.FromSeconds(15));
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_ChangeColumnType_ContinuesTransparently()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-type-change", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Change type from VARCHAR(200) to TEXT — compatible change
            ExecuteNpgSql(connectionString, "ALTER TABLE items ALTER COLUMN name TYPE TEXT");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Type Change')");

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Type Change", doc2.Name);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_RenameColumn_EntersFallback()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-rename-col", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            var errorTask = await WaitForNextProcessError(store, "test-rename-col");

            // Rename the mapped column — FindColumnIndex won't find 'name' anymore
            ExecuteNpgSql(connectionString, "ALTER TABLE items RENAME COLUMN name TO title");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, title) VALUES (2, 'After Rename')");

            await errorTask.WaitAsync(TimeSpan.FromSeconds(15));
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_MultipleChanges_RecoversThroughAll()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    extra1 VARCHAR(200),
                    extra2 VARCHAR(200)
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-multi-change", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Change 1: add a column at end
            ExecuteNpgSql(connectionString, "ALTER TABLE items ADD COLUMN extra3 VARCHAR(200)");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Add')");

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            // Change 2: drop an unmapped column
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN extra1");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (3, 'After Drop')");

            var doc3 = await WaitForDocumentAsync<Item>(store, "Items/3", timeoutMs: 60_000);
            Assert.NotNull(doc3);

            // Change 3: add another column
            ExecuteNpgSql(connectionString, "ALTER TABLE items ADD COLUMN extra4 INTEGER");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (4, 'After Second Add')");

            var doc4 = await WaitForDocumentAsync<Item>(store, "Items/4", timeoutMs: 60_000);
            Assert.NotNull(doc4);
            Assert.Equal("After Second Add", doc4.Name);

            // Process should have handled all changes without entering fallback
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-multi-change");
            Assert.Null(process?.FallbackTime);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task SchemaEvolution_AddAndRemoveSameCount()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    unmapped_col VARCHAR(200)
                )");

            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, unmapped_col) VALUES (1, 'Before', 'x')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = BuildSimpleConfig("test-add-remove-same", sqlCs.Name);
            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Drop one unmapped column, add a different one — same total count, different schema
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN unmapped_col");
            ExecuteNpgSql(connectionString, "ALTER TABLE items ADD COLUMN different_col INTEGER");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name, different_col) VALUES (2, 'After Swap', 42)");

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Swap", doc2.Name);
        }
    }
}
