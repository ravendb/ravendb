using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Server.Documents.CdcSink.Schema;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkMySqlSchemaDiscoveryTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkMySqlSchemaDiscoveryTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMySql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.MySQL_MySqlConnector, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task DiscoversTablesColumnsPrimaryKeysAndForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE customers (
                    id      INT AUTO_INCREMENT PRIMARY KEY,
                    name    VARCHAR(200) NOT NULL,
                    metadata JSON,
                    photo   BLOB
                );");
            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    order_id    INT AUTO_INCREMENT PRIMARY KEY,
                    customer_id INT NOT NULL,
                    total       DECIMAL(12,2) NOT NULL,
                    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
                );");
            ExecuteMySql(connectionString, @"
                CREATE TABLE order_items (
                    order_id INT NOT NULL,
                    line_no  INT NOT NULL,
                    qty      INT NOT NULL,
                    PRIMARY KEY (order_id, line_no),
                    CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
                );");

            var discovery = CdcSinkSchemaDiscovery.For("MySqlConnector.MySqlConnectorFactory");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            var customers = schema.Tables.Single(t => t.SourceTableName == "customers");
            Assert.True(customers.IsCdcEnabled);
            Assert.Equal(new[] { "id" }, customers.PrimaryKeyColumns);

            var metadata = customers.Columns.Single(c => c.Name == "metadata");
            Assert.Equal("json", metadata.NativeType);
            Assert.Equal(CdcColumnType.Json, metadata.SuggestedType);

            var photo = customers.Columns.Single(c => c.Name == "photo");
            Assert.Equal("blob", photo.NativeType);
            Assert.Equal(CdcColumnType.Attachment, photo.SuggestedType);

            var orders = schema.Tables.Single(t => t.SourceTableName == "orders");
            var ordersFk = Assert.Single(orders.ForeignKeys);
            Assert.Equal(new[] { "customer_id" }, ordersFk.Columns);
            Assert.Equal("customers", ordersFk.ReferencedTable);
            Assert.Equal(new[] { "id" }, ordersFk.ReferencedColumns);

            var items = schema.Tables.Single(t => t.SourceTableName == "order_items");
            Assert.Equal(new[] { "order_id", "line_no" }, items.PrimaryKeyColumns);
            Assert.True(items.Columns.All(c => c.IsCdcCapturable));
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task GeneratedColumnsAreMarkedNotCapturable()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE products (
                    id            INT AUTO_INCREMENT PRIMARY KEY,
                    price         DECIMAL(12,2) NOT NULL,
                    qty           INT NOT NULL,
                    total_stored  DECIMAL(20,2) AS (price * qty) STORED,
                    total_virtual DECIMAL(20,2) AS (price * qty) VIRTUAL,
                    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expr_default  INT NOT NULL DEFAULT (1 + 1)
                );");

            var discovery = CdcSinkSchemaDiscovery.For("MySqlConnector.MySqlConnectorFactory");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            var products = schema.Tables.Single(t => t.SourceTableName == "products");
            Assert.True(products.Columns.Single(c => c.Name == "price").IsCdcCapturable);

            var stored = products.Columns.Single(c => c.Name == "total_stored");
            Assert.False(stored.IsCdcCapturable);
            Assert.False(string.IsNullOrEmpty(stored.UnsupportedReason));

            var virtualColumn = products.Columns.Single(c => c.Name == "total_virtual");
            Assert.False(virtualColumn.IsCdcCapturable);
            Assert.False(string.IsNullOrEmpty(virtualColumn.UnsupportedReason));

            // Regular columns with an expression default report EXTRA = DEFAULT_GENERATED on MySQL 8.0.13+,
            // but they are ordinary columns present in the binlog row image - they must stay capturable.
            Assert.True(products.Columns.Single(c => c.Name == "created_at").IsCdcCapturable);
            Assert.True(products.Columns.Single(c => c.Name == "expr_default").IsCdcCapturable);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task DiscoversCrossDatabaseForeignKey()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MySQL_MySqlConnector, out var connectionString, out _, dataSet: null, includeData: false);

            var otherDb = "cdc_xdb_" + Guid.NewGuid().ToString("N");
            try
            {
                ExecuteMySql(connectionString, $"CREATE DATABASE `{otherDb}`");
                ExecuteMySql(connectionString, $"CREATE TABLE `{otherDb}`.parent_other (id INT PRIMARY KEY)");
                ExecuteMySql(connectionString, $@"
                    CREATE TABLE child_main (
                        cid INT PRIMARY KEY,
                        ref INT NOT NULL,
                        CONSTRAINT fk_cross FOREIGN KEY (ref) REFERENCES `{otherDb}`.parent_other(id)
                    );");

                var discovery = CdcSinkSchemaDiscovery.For("MySqlConnector.MySqlConnectorFactory");
                var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

                var child = Assert.Single(schema.Tables);
                Assert.Equal("child_main", child.SourceTableName);

                var fk = Assert.Single(child.ForeignKeys);
                Assert.Equal(new[] { "ref" }, fk.Columns);
                Assert.Equal(otherDb, fk.ReferencedSchema);
                Assert.Equal("parent_other", fk.ReferencedTable);
                Assert.Equal(new[] { "id" }, fk.ReferencedColumns);
            }
            finally
            {
                ExecuteMySql(connectionString, "DROP TABLE IF EXISTS child_main");
                ExecuteMySql(connectionString, $"DROP DATABASE IF EXISTS `{otherDb}`");
            }
        }
    }
}
