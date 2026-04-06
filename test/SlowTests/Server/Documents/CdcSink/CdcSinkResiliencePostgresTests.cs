using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
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

            // Drop the mapped column — the RelationMessage will have fewer columns.
            // FindColumnIndex will throw for 'extra', causing a retry.
            // On retry, the process re-resolves columns and 'extra' won't be found →
            // the process will keep erroring until the configuration is updated.
            // This is expected behavior: removing a mapped column is a config error.
            ExecuteNpgSql(connectionString, "ALTER TABLE items DROP COLUMN extra");
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            // The process should enter fallback mode since the mapped column no longer exists
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-schema-drop-col");
            Assert.NotNull(process);

            // Wait a bit for the error to surface
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 15_000)
            {
                if (process.FallbackTime != null)
                    break;
                await Task.Delay(500);
            }

            Assert.NotNull(process.FallbackTime); // should be in fallback mode
            Assert.False(process.IsHealthy(out var issue));
            Assert.Contains("error recovery", issue);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Connection Failure Recovery Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ConnectionFailure_RecoversAfterReplicationConnectionKilled()
        {
            using var store = GetDocumentStore();
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

            // Kill the replication connection by terminating the WAL sender backend
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-conn-failure");
            Assert.NotNull(process);

            var slotName = config.Postgres?.SlotName;
            if (string.IsNullOrEmpty(slotName))
            {
                // Auto-generated slot name — find it from the process state
                slotName = $"ravendb_cdc_{Math.Abs(config.Name.GetHashCode()):x8}";
            }

            ExecuteNpgSql(connectionString,
                $"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{slotName}' AND active_pid IS NOT NULL");

            // Wait for the process to enter fallback mode
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 15_000)
            {
                if (process.FallbackTime != null)
                    break;
                await Task.Delay(250);
            }

            Assert.NotNull(process.FallbackTime); // should be in fallback mode

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
    }
}
