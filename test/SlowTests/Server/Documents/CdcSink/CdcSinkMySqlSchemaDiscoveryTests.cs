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
                    total_virtual DECIMAL(20,2) AS (price * qty) VIRTUAL
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
        }
    }
}
