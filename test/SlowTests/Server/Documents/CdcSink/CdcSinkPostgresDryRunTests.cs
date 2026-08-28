using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSinkPostgresDryRunTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresDryRunTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, connectionString, sql);
        }

        private static async Task<object> QueryScalarAsync(string connectionString, string sql, string paramName = null, object paramValue = null)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            if (paramName != null)
                cmd.Parameters.AddWithValue(paramName, paramValue);
            return await cmd.ExecuteScalarAsync();
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "pg-cdc-dry-run")
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

        private static CdcSinkConfiguration BuildProductsConfig(string connectionStringName)
        {
            return new CdcSinkConfiguration
            {
                Name = "live-task",
                ConnectionStringName = connectionStringName,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "price", Name = "Price" }
                        }
                    }
                }
            };
        }

        private async Task<(CdcSinkConfiguration SavedConfig, int WalSenderPid)> StartLiveTaskAndGetStreamingStateAsync(
            IDocumentStore store, string connectionString, SqlConnectionString sqlCs)
        {
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price NUMERIC(12,2) NOT NULL
                )");
            ExecuteNpgSql(connectionString, "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);");

            var config = BuildProductsConfig(sqlCs.Name);
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, config.Name);

            ExecuteNpgSql(connectionString, "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);");
            Assert.NotNull(await WaitForSinkDocumentAsync<Product>(store, config.Name, "Products/2"));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var savedConfig = record.CdcSinks.Single(x => x.Name == config.Name);
            Assert.NotNull(savedConfig.Postgres?.SlotName);
            Assert.NotNull(savedConfig.Postgres?.PublicationName);

            var pid = await QueryScalarAsync(connectionString,
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = @slot", "slot", savedConfig.Postgres.SlotName);
            Assert.NotNull(pid);

            return (savedConfig, (int)pid);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task DryRun_AgainstLiveTask_DoesNotTerminateItsWalSender()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var (savedConfig, pidBefore) = await StartLiveTaskAndGetStreamingStateAsync(store, connectionString, sqlCs);
            var errorTask = await WaitForNextProcessError(store, savedConfig.Name);

            var result = await store.Maintenance.SendAsync(new VerifyCdcSinkOperation(new CdcTestRequest
            {
                Configuration = savedConfig,
                Connection = sqlCs
            }));

            Assert.True(result.Success, result.Error);

            var pidAfter = await QueryScalarAsync(connectionString,
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = @slot", "slot", savedConfig.Postgres.SlotName);
            Assert.Equal(pidBefore, pidAfter);

            ExecuteNpgSql(connectionString, "INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99);");
            Assert.NotNull(await WaitForSinkDocumentAsync<Product>(store, savedConfig.Name, "Products/3"));

            Assert.False(errorTask.IsCompleted, errorTask.IsCompleted ? errorTask.Result?.ToString() : null);

            var publicationExists = await QueryScalarAsync(connectionString,
                "SELECT 1 FROM pg_publication WHERE pubname = @pub", "pub", savedConfig.Postgres.PublicationName);
            Assert.NotNull(publicationExists);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task DryRun_CreatesTemporarySlot_AndLeavesNoSlotBehind()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price NUMERIC(12,2) NOT NULL
                );
                INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
                CREATE PUBLICATION dryrun_pub FOR TABLE products;");

            var config = BuildProductsConfig(connectionStringName: null);
            config.Name = "dry-run-only";
            config.Postgres = new CdcSinkPostgresSettings
            {
                PublicationName = "dryrun_pub",
                SlotName = "rvn_dryrun_slot_c2"
            };

            await using var lockConn = new NpgsqlConnection(connectionString);
            await lockConn.OpenAsync();
            await using var lockTx = await lockConn.BeginTransactionAsync();
            await using (var lockCmd = new NpgsqlCommand("LOCK TABLE products IN ACCESS EXCLUSIVE MODE", lockConn, lockTx))
                await lockCmd.ExecuteNonQueryAsync();

            var dryRunTask = store.Maintenance.SendAsync(new VerifyCdcSinkOperation(new CdcTestRequest
            {
                Configuration = config,
                Connection = new SqlConnectionString
                {
                    Name = "inline",
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString
                }
            }));

            object temporary = null;
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 60_000)
            {
                temporary = await QueryScalarAsync(connectionString,
                    "SELECT temporary FROM pg_replication_slots WHERE slot_name = @slot", "slot", config.Postgres.SlotName);
                if (temporary != null || dryRunTask.IsCompleted)
                    break;

                await Task.Delay(100);
            }

            if (temporary == null && dryRunTask.IsCompleted)
            {
                var early = await dryRunTask;
                Assert.Fail($"Dry-run completed before the slot was observed. Success={early.Success}, Error={early.Error}");
            }

            Assert.NotNull(temporary);
            Assert.True((bool)temporary);

            await lockTx.RollbackAsync();

            var result = await dryRunTask;
            Assert.True(result.Success, result.Error);

            var slotAfter = await QueryScalarAsync(connectionString,
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot", "slot", config.Postgres.SlotName);
            Assert.Null(slotAfter);

            var publicationAfter = await QueryScalarAsync(connectionString,
                "SELECT 1 FROM pg_publication WHERE pubname = @pub", "pub", config.Postgres.PublicationName);
            Assert.NotNull(publicationAfter);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlCdcRequired = true)]
        public async Task DryRun_WithAddedTable_DoesNotAlterLivePublication()
        {
            using var store = GetDocumentStore();
            using var teardown = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var (savedConfig, pidBefore) = await StartLiveTaskAndGetStreamingStateAsync(store, connectionString, sqlCs);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE extras (id SERIAL PRIMARY KEY, label VARCHAR(50));
                INSERT INTO extras (id, label) VALUES (1, 'X');");

            savedConfig.Tables.Add(new CdcSinkTableConfig
            {
                CollectionName = "Extras",
                SourceTableSchema = "public",
                SourceTableName = "extras",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "DbId" },
                    new CdcColumnMapping { Column = "label", Name = "Label" }
                }
            });

            var result = await store.Maintenance.SendAsync(new VerifyCdcSinkOperation(new CdcTestRequest
            {
                Configuration = savedConfig,
                Connection = sqlCs
            }));

            Assert.True(result.Success, result.Error);
            Assert.Contains(result.Warnings, w => w.Contains("extras") && w.Contains(savedConfig.Postgres.PublicationName));

            var extrasPublished = await QueryScalarAsync(connectionString, @"
                SELECT 1
                FROM pg_publication_rel pr
                JOIN pg_class c ON c.oid = pr.prrelid
                WHERE c.relname = 'extras'
                  AND pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = @pub)", "pub", savedConfig.Postgres.PublicationName);
            Assert.Null(extrasPublished);

            var pidAfter = await QueryScalarAsync(connectionString,
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = @slot", "slot", savedConfig.Postgres.SlotName);
            Assert.Equal(pidBefore, pidAfter);
        }
    }
}
