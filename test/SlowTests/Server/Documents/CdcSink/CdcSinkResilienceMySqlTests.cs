using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkMySqlTests))]
    public class CdcSinkResilienceMySqlTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkResilienceMySqlTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMySql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, connectionString, sql);
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "mysql-resilience-test")
        {
            var sqlCs = new SqlConnectionString
            {
                Name = name,
                FactoryName = "MySqlConnector.MySqlConnectorFactory",
                ConnectionString = connectionString
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Schema Evolution Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task SchemaEvolution_AddColumn_DetectedViaTableMapEvent()
        {
            // MySQL sends TableMapEvent before each row event. When a column is added,
            // the TableMapEvent will have a different column count than our cached metadata.
            // The process should detect this mismatch and restart to re-learn the schema.

            using var store = GetDocumentStore();
            using var __ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-schema-add-col",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
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

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Add a column — MySQL will send a new TableMapEvent with 3 columns
            // Our cached BinlogColumnTypes has 2. The SequenceEqual check detects the mismatch.
            ExecuteMySql(connectionString, "ALTER TABLE items ADD COLUMN description VARCHAR(500)");
            ExecuteMySql(connectionString, "INSERT INTO items (id, name, description) VALUES (2, 'After', 'new col')");

            // The process should detect the schema change, enter fallback, then recover
            // with updated metadata. The new column isn't in our mapping so it's ignored,
            // but the row should arrive.
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After", doc2.Name);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task SchemaEvolution_RemoveColumn_DetectedViaTableMapEvent()
        {
            using var store = GetDocumentStore();
            using var __ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(200) NOT NULL,
                    extra VARCHAR(200)
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (id, name, extra) VALUES (1, 'Before', 'will drop')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-schema-drop-col",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
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

            // Drop the column — TableMapEvent will have fewer columns
            ExecuteMySql(connectionString, "ALTER TABLE items DROP COLUMN extra");

            // Force MySQL to deliver the new TableMapEvent (with fewer columns) through the binlog.
            // The ALTER TABLE DDL alone doesn't generate a row event; only a subsequent DML triggers
            // the TableMapEvent that the CDC client needs to detect the schema change.
            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (99, 'flush')");
            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            // Process should detect the mismatch and enter fallback.
            // On retry, it re-resolves columns, but 'extra' is gone from the table
            // while still in the config → FindColumnIndex throws → stays in fallback.
            await errorTask.WaitAsync(TimeSpan.FromSeconds(60));
        }

        // ─────────────────────────────────────────────────────────────────────
        // Connection Failure Recovery Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ConnectionFailure_RecoversAfterStopAndRestart()
        {
            using var store = GetDocumentStore();
            using var __ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before Stop')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-conn-failure",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
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

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Item>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Stop the process to simulate a connection failure
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-conn-failure");
            Assert.NotNull(process);
            process.Stop("test: simulating failure");

            // Insert while stopped
            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'During Stop')");

            // Restart — should recover from saved GTID and pick up the new row
            process.Start();

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("During Stop", doc2.Name);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task SchemaChange_FalsePositive_OnStableSchemaWithDateColumn()
        {

            using var store = GetDocumentStore();
            using var __ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    d  DATE
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (d) VALUES ('2026-01-01')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-schema-false-positive-date",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "d",  Name = "Date" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Initial load arrives — proves the sink is healthy at this point.
            var firstDoc = await WaitForDocumentAsync<dynamic>(store, "Items/1", timeoutMs: 60_000);
            Assert.NotNull(firstDoc);

            // Subscribe to ProcessError before the post-init INSERT to catch the bug
            // deterministically; race it against the document arriving.
            var errorTask = await WaitForNextProcessError(store, config.Name);

            ExecuteMySql(connectionString, "INSERT INTO items (d) VALUES ('2026-02-02')");

            var docTask = WaitForDocumentAsync<dynamic>(store, "Items/2", timeoutMs: 60_000);
            var winner = await Task.WhenAny(errorTask, docTask);

            if (winner == errorTask)
            {
                var ex = await errorTask;
                Assert.Fail(
                    "Schema-change false-positive on stable schema with DATE column. " +
                    "MySqlDataTypeToBinlogType[\"date\"] disagrees with the binlog wire format. " +
                    $"Exception: {ex}");
            }

            Assert.NotNull(await docTask);
        }
    }
}
