using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSinkPostgresTestMappingTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresTestMappingTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, sql);
        }

        private async Task<(DocumentDatabase Db, DocumentsOperationContext Ctx, Raven.Client.Documents.IDocumentStore Store)> SetupAsync()
        {
            var store = GetDocumentStore();
            var db = await GetDatabase(store.Database);
            db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);
            return (db, ctx, store);
        }

        private static CdcSinkTableConfig BuildLecturersTableConfig()
        {
            return new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                    new() { Column = "email", Name = "Email" },
                },
            };
        }

        private static async Task<TestCdcSinkMappingResult> RunAsync(
            DocumentDatabase db,
            DocumentsOperationContext ctx,
            string connectionString,
            CdcSinkConfiguration config,
            CdcSinkTableConfig table,
            TestCdcSinkRowSelector selector,
            string[] pkValues,
            TestCdcSinkOperation op,
            int maxRows = 1)
        {
            var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.NpgSQL, connectionString);
            var fetched = await driver.FetchRowsAsync(
                table.SourceTableSchema, table.SourceTableName, table.PrimaryKeyColumns,
                selector == TestCdcSinkRowSelector.First ? RowFetchMode.First : RowFetchMode.ByPrimaryKey,
                pkValues, maxRows, db.DatabaseShutdown);

            return CdcSinkTestRunner.Run(db, ctx, config, table, fetched.ColumnNames, fetched.Rows, op);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task HappyPath_SingleRow_Upsert()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Upsert, maxRows: 1);

            Assert.Empty(result.Errors);
            Assert.Single(result.Results);

            var row = result.Results[0];
            Assert.Null(row.Error);
            Assert.Equal("Lecturers/1", row.DocumentId);
            Assert.False(row.WouldDelete);
            Assert.NotNull(row.Document);
            Assert.NotNull(row.Document);
            Assert.Contains("\"Name\":\"Alice\"", row.Document);
            Assert.Contains("\"Email\":\"alice@example.com\"", row.Document);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task MultiRow_OrderedByPrimaryKey()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (3, 'C', 'c@x');
                INSERT INTO lecturers (id, name, email) VALUES (1, 'A', 'a@x');
                INSERT INTO lecturers (id, name, email) VALUES (5, 'E', 'e@x');
                INSERT INTO lecturers (id, name, email) VALUES (2, 'B', 'b@x');
                INSERT INTO lecturers (id, name, email) VALUES (4, 'D', 'd@x');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Upsert, maxRows: 3);

            Assert.Empty(result.Errors);
            Assert.Equal(3, result.Results.Count);
            Assert.Equal(new[] { "Lecturers/1", "Lecturers/2", "Lecturers/3" }, result.Results.Select(r => r.DocumentId).ToArray());
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchScript_AppliedToDocument()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            table.Patch = "this.Greeting = 'hello ' + $row.name;";
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Upsert, maxRows: 1);

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.Null(row.Error);
            Assert.Contains("\"Greeting\":\"hello Alice\"", row.Document);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ByPrimaryKey_FetchesMatchingRow()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (42, 'Forty-Two', 'fortytwo@example.com');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.ByPrimaryKey, pkValues: new[] { "42" }, TestCdcSinkOperation.Upsert);

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.Equal("Lecturers/42", row.DocumentId);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PerRowErrorIsolation_PatchThrowsOnOneRow()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'A', 'a@x');
                INSERT INTO lecturers (id, name, email) VALUES (2, 'B', 'b@x');
                INSERT INTO lecturers (id, name, email) VALUES (3, 'C', 'c@x');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            // Throw on the row with id == 2; the other two should still succeed.
            table.Patch = "if ($row.id === 2) throw 'boom'; this.PatchedBy = 'test';";
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Upsert, maxRows: 3);

            Assert.Empty(result.Errors);
            Assert.Equal(3, result.Results.Count);
            Assert.Null(result.Results[0].Error);
            Assert.NotNull(result.Results[1].Error);
            Assert.Contains("boom", result.Results[1].Error);
            Assert.Null(result.Results[2].Error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task DeleteMode_ReportsWouldDeleteAndRunsOnDeletePatch()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            table.OnDelete = new CdcSinkOnDeleteConfig
            {
                Patch = "output('delete called for ' + $row.id);"
            };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Delete, maxRows: 1);

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.True(row.WouldDelete);
            Assert.False(row.IgnoreDeletes);
            Assert.NotNull(row.DebugOutput);
            Assert.Contains(row.DebugOutput, line => line.Contains("delete called for 1"));
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task DeleteMode_IgnoreDeletes_FlippedFlag()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            table.OnDelete = new CdcSinkOnDeleteConfig { IgnoreDeletes = true };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Delete, maxRows: 1);

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.False(row.WouldDelete);
            Assert.True(row.IgnoreDeletes);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task LinkedOrEmbeddedTables_SurfacesAdvisoryInWarningsNotErrors()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200), dept_id INT);
                INSERT INTO lecturers (id, name, email, dept_id) VALUES (1, 'Alice', 'alice@example.com', 7);");

            var (db, ctx, _) = await SetupAsync();
            var table = BuildLecturersTableConfig();
            // BuildLecturersTableConfig only configures id/name/email; the linked table's join
            // column must also be in Columns so SetSourceColumnNames accepts the layout.
            table.Columns.Add(new CdcColumnMapping { Column = "dept_id", Name = "DeptId" });
            table.LinkedTables = new List<CdcSinkLinkedTableConfig>
            {
                new()
                {
                    SourceTableSchema = "public",
                    SourceTableName = "departments",
                    PropertyName = "Department",
                    JoinColumns = new List<string> { "dept_id" },
                    LinkedCollectionName = "Departments",
                },
            };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var result = await RunAsync(db, ctx, connectionString, config, table,
                TestCdcSinkRowSelector.First, pkValues: null, TestCdcSinkOperation.Upsert, maxRows: 1);

            // Results still populated — the root mapping ran fine.
            Assert.Empty(result.Errors);
            Assert.Single(result.Results);

            // The "linked/embedded not exercised" note is advisory and must land in Warnings, not Errors.
            var warning = Assert.Single(result.Warnings);
            Assert.Contains("Linked and embedded tables", warning);
        }
    }
}
