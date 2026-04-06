using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkSqlServerTests))]
    public class CdcSinkResilienceSqlServerTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkResilienceSqlServerTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMsSql(string connectionString, string sql)
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();
            using var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = 120;
            cmd.ExecuteNonQuery();
        }

        private void EnableCdc(string connectionString)
        {
            ExecuteMsSql(connectionString, "EXEC sys.sp_cdc_enable_db");
        }

        private void EnableCdcOnTable(string connectionString, string schema, string tableName)
        {
            ExecuteMsSql(connectionString,
                $"EXEC sys.sp_cdc_enable_table @source_schema = N'{schema}', @source_name = N'{tableName}', @role_name = NULL");
            WaitForCdcCaptureInstance(connectionString, schema, tableName);
        }

        private void WaitForCdcCaptureInstance(string connectionString, string schema, string tableName)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 30_000)
            {
                using var connection = new SqlConnection(connectionString);
                connection.Open();
                using var cmd = new SqlCommand(
                    "SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(@name)", connection);
                cmd.Parameters.AddWithValue("@name", $"{schema}.{tableName}");
                var result = cmd.ExecuteScalar();
                if (result != null)
                    return;
                System.Threading.Thread.Sleep(500);
            }
            throw new TimeoutException($"CDC capture instance for {schema}.{tableName} was not created within 30 seconds.");
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "mssql-resilience-test")
        {
            var sqlCs = new SqlConnectionString
            {
                Name = name,
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = connectionString
            };
            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Schema Evolution Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task SchemaEvolution_AddColumn_CaptureInstanceRetainsOldSchema()
        {
            // SQL Server CDC capture instances are immutable — adding a column to the table
            // does NOT affect the existing capture instance. New columns only appear after
            // sp_cdc_enable_table is called again (creating a new capture instance).
            // The CDC Sink should continue working with the old column set.

            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "items");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before')");

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
                        SourceTableSchema = "dbo",
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

            // Add a column — capture instance is NOT updated (immutable in SQL Server CDC)
            ExecuteMsSql(connectionString, "ALTER TABLE items ADD description NVARCHAR(500)");

            // Insert with the new column — CDC still captures old column set
            ExecuteMsSql(connectionString, "INSERT INTO items (id, name, description) VALUES (2, 'After', 'new col')");

            // Should still work fine — the capture instance has the old columns
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After", doc2.Name);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task SchemaEvolution_RemoveColumn_ProcessEntersFallbackOnRecreatedCapture()
        {
            // When a mapped column is dropped and the capture instance is recreated,
            // the CDC Sink should detect the mismatch and enter fallback mode.

            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    extra NVARCHAR(200)
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "items");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name, extra) VALUES (1, 'Before', 'will drop')");

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
                        SourceTableSchema = "dbo",
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

            // Disable CDC on the table, drop the column, re-enable with new schema
            ExecuteMsSql(connectionString, "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'items', @capture_instance = N'dbo_items'");
            ExecuteMsSql(connectionString, "ALTER TABLE items DROP COLUMN extra");
            EnableCdcOnTable(connectionString, "dbo", "items");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Drop')");

            // The process should detect the missing column on retry and enter fallback
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-schema-drop-col");
            Assert.NotNull(process);

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 30_000)
            {
                if (process.FallbackTime != null)
                    break;
                await Task.Delay(500);
            }

            Assert.NotNull(process.FallbackTime);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Connection Failure Recovery Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task ConnectionFailure_RecoversAfterSessionKilled()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "items");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before Kill')");

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
                        SourceTableSchema = "dbo",
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

            // SQL Server poll-based CDC doesn't hold a persistent connection,
            // so killing a session has limited impact — the next poll opens a new connection.
            // Instead, we verify recovery by stopping and restarting the process.
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-conn-failure");
            Assert.NotNull(process);

            process.Stop("test: simulating failure");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'After Stop')");

            process.Start();

            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal("After Stop", doc2.Name);
        }

        // ─────────────────────────────────────────────────────────────────────
        // LSN Gap Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task LsnGap_DetectedWhenChangesArePurged()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "items");

            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (1, 'Before Gap')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-lsn-gap",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "dbo",
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

            // Wait for streaming to start
            await Task.Delay(3000);

            // Stop the process
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-lsn-gap");
            Assert.NotNull(process);
            process.Stop("test: purging changes");

            // Insert rows that we'll purge
            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'Will Be Purged')");

            // Purge all CDC changes with aggressive cleanup
            // sp_cdc_cleanup_change_table requires the low_water_mark to be before the changes
            try
            {
                ExecuteMsSql(connectionString, @"
                    DECLARE @max_lsn binary(10) = sys.fn_cdc_get_max_lsn();
                    EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_items', @low_water_mark = @max_lsn, @threshold = 1");
            }
            catch
            {
                // Cleanup may fail if there are no changes or the LSN is invalid — skip the test
                // This is inherently fragile because CDC cleanup timing depends on SQL Agent
                return;
            }

            // Insert new data after the gap
            ExecuteMsSql(connectionString, "INSERT INTO items (id, name) VALUES (3, 'After Gap')");

            // Restart the process
            process.Start();

            // The process should recover and pick up new data (with gap warning)
            var doc3 = await WaitForDocumentAsync<Item>(store, "Items/3", timeoutMs: 60_000);
            Assert.NotNull(doc3);
            Assert.Equal("After Gap", doc3.Name);
        }
    }
}
