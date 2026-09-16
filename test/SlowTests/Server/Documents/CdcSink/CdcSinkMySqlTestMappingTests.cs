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
    public class CdcSinkMySqlTestMappingTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkMySqlTestMappingTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMySql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.MySQL_MySqlConnector, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task HappyPath_SingleRow_Upsert_UsesMySqlLimit()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price DECIMAL(12,2) NOT NULL
                );");
            ExecuteMySql(connectionString, @"
                INSERT INTO products (name, price) VALUES ('Alpha', 9.99);
                INSERT INTO products (name, price) VALUES ('Beta', 19.99);");

            using var store = GetDocumentStore();
            var db = await GetDatabase(store.Database);
            using var ctxScope = db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx);

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Products",
                SourceTableSchema = schemaName,
                SourceTableName = "products",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                    new() { Column = "price", Name = "Price" },
                },
            };
            var config = new CdcSinkConfiguration { Name = "test", Tables = new List<CdcSinkTableConfig> { table } };

            var driver = DatabaseDriverDispatcher.CreateDriver(MigrationProvider.MySQL_MySqlConnector, connectionString);
            var fetched = await driver.FetchRowsAsync(
                table.SourceTableSchema, table.SourceTableName, table.PrimaryKeyColumns,
                RowFetchMode.First, primaryKeyValues: null, maxRows: 2, db.DatabaseShutdown);

            var result = CdcSinkTestRunner.Run(db, ctx, config, table, fetched.ColumnNames, fetched.Rows, TestCdcSinkOperation.Upsert, defaultSchema: table.SourceTableSchema);

            Assert.Empty(result.Errors);
            Assert.Equal(2, result.Results.Count);
            Assert.Equal("Products/1", result.Results[0].DocumentId);
            Assert.Contains("\"Name\":\"Alpha\"", result.Results[0].Document);
        }
    }
}
