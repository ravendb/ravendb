using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkSqlServerTestMappingTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkSqlServerTestMappingTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMsSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task HappyPath_SingleRow_Upsert_AppliesPatchAndUsesTopN()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE products (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price DECIMAL(12,2) NOT NULL
                );");
            ExecuteMsSql(connectionString, @"
                INSERT INTO products (name, price) VALUES ('Alpha', 9.99);
                INSERT INTO products (name, price) VALUES ('Beta', 19.99);
                INSERT INTO products (name, price) VALUES ('Gamma', 29.99);");

            using var store = GetDocumentStore();
            var db = await GetDatabase(store.Database);
            db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Products",
                SourceTableSchema = "dbo",
                SourceTableName = "products",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                    new() { Column = "price", Name = "Price" },
                },
                Patch = "this.Label = $row.name + ' - $' + $row.price;",
            };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MsSQL, connectionString);
            var fetched = await driver.FetchRowsAsync(
                table.SourceTableSchema, table.SourceTableName, table.PrimaryKeyColumns,
                RowFetchMode.First, primaryKeyValues: null, maxRows: 2, db.DatabaseShutdown);

            var result = CdcSinkTestRunner.Run(db, ctx, config, table, fetched.ColumnNames, fetched.Rows, TestCdcSinkOperation.Upsert);

            Assert.Empty(result.Errors);
            Assert.Equal(2, result.Results.Count);
            Assert.Null(result.Results[0].Error);
            Assert.Contains("\"Label\":\"Alpha", result.Results[0].Document);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task ByPrimaryKey_CoercesIntegerType()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE widgets (id INT PRIMARY KEY, label VARCHAR(100));
                INSERT INTO widgets (id, label) VALUES (101, 'one-oh-one');");

            using var store = GetDocumentStore();
            var db = await GetDatabase(store.Database);
            db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Widgets",
                SourceTableSchema = "dbo",
                SourceTableName = "widgets",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "label", Name = "Label" },
                },
            };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MsSQL, connectionString);
            var fetched = await driver.FetchRowsAsync(
                table.SourceTableSchema, table.SourceTableName, table.PrimaryKeyColumns,
                RowFetchMode.ByPrimaryKey, primaryKeyValues: new[] { "101" }, maxRows: 1, db.DatabaseShutdown);

            var result = CdcSinkTestRunner.Run(db, ctx, config, table, fetched.ColumnNames, fetched.Rows, TestCdcSinkOperation.Upsert);

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.Equal("Widgets/101", row.DocumentId);
        }
    }
}
