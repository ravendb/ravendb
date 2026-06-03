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
    public class CdcSinkPostgresSchemaDiscoveryTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresSchemaDiscoveryTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task DiscoversTablesColumnsPrimaryKeysAndForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE customers (
                    id           SERIAL PRIMARY KEY,
                    name         VARCHAR(200) NOT NULL,
                    metadata     JSONB,
                    photo        BYTEA
                );

                CREATE TABLE orders (
                    order_id     SERIAL PRIMARY KEY,
                    customer_id  INTEGER NOT NULL REFERENCES customers(id),
                    total        NUMERIC(12,2) NOT NULL,
                    payload      JSON
                );

                CREATE TABLE order_items (
                    order_id     INTEGER NOT NULL REFERENCES orders(order_id),
                    line_no      INTEGER NOT NULL,
                    qty          INTEGER NOT NULL,
                    PRIMARY KEY (order_id, line_no)
                );
            ");

            var discovery = CdcSinkSchemaDiscovery.For("Npgsql");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            Assert.NotNull(schema.CatalogName);
            Assert.Equal(3, schema.Tables.Count);

            var customers = schema.Tables.Single(t => t.SourceTableName == "customers");
            Assert.Equal("public", customers.SourceTableSchema);
            Assert.True(customers.IsCdcEnabled);
            Assert.Equal(new[] { "id" }, customers.PrimaryKeyColumns);

            var id = customers.Columns.Single(c => c.Name == "id");
            Assert.True(id.IsPrimaryKey);
            Assert.Equal("integer", id.NativeType);
            Assert.Equal(CdcColumnType.Default, id.SuggestedType);
            Assert.True(id.IsCdcCapturable);

            var metadata = customers.Columns.Single(c => c.Name == "metadata");
            Assert.Equal("jsonb", metadata.NativeType);
            Assert.Equal(CdcColumnType.Json, metadata.SuggestedType);

            var photo = customers.Columns.Single(c => c.Name == "photo");
            Assert.Equal("bytea", photo.NativeType);
            Assert.Equal(CdcColumnType.Attachment, photo.SuggestedType);

            var orders = schema.Tables.Single(t => t.SourceTableName == "orders");
            Assert.Equal(new[] { "order_id" }, orders.PrimaryKeyColumns);
            var payload = orders.Columns.Single(c => c.Name == "payload");
            Assert.Equal("json", payload.NativeType);
            Assert.Equal(CdcColumnType.Json, payload.SuggestedType);

            // orders -> customers FK
            var ordersFk = Assert.Single(orders.ForeignKeys);
            Assert.Equal(new[] { "customer_id" }, ordersFk.Columns);
            Assert.Equal("public", ordersFk.ReferencedSchema);
            Assert.Equal("customers", ordersFk.ReferencedTable);
            Assert.Equal(new[] { "id" }, ordersFk.ReferencedColumns);

            // order_items -> orders FK with composite PK + composite FK columns
            var items = schema.Tables.Single(t => t.SourceTableName == "order_items");
            Assert.Equal(new[] { "order_id", "line_no" }, items.PrimaryKeyColumns);
            var itemsFk = Assert.Single(items.ForeignKeys);
            Assert.Equal(new[] { "order_id" }, itemsFk.Columns);
            Assert.Equal("orders", itemsFk.ReferencedTable);
            Assert.Equal(new[] { "order_id" }, itemsFk.ReferencedColumns);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task RespectsSchemasFilter()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE SCHEMA shop;
                CREATE TABLE shop.items (id SERIAL PRIMARY KEY, name VARCHAR(100));
                CREATE TABLE public.unrelated (id SERIAL PRIMARY KEY, junk VARCHAR(100));
            ");

            var discovery = CdcSinkSchemaDiscovery.For("Npgsql");

            var shopOnly = await discovery.DiscoverAsync(connectionString, new[] { "shop" }, CancellationToken.None);
            Assert.Single(shopOnly.Tables);
            Assert.Equal("items", shopOnly.Tables[0].SourceTableName);

            // No schemas hint => defaults to ["public"]
            var publicOnly = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);
            Assert.Single(publicOnly.Tables);
            Assert.Equal("unrelated", publicOnly.Tables[0].SourceTableName);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task GeneratedColumnsAreMarkedNotCapturable()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id    SERIAL PRIMARY KEY,
                    price NUMERIC(12,2) NOT NULL,
                    qty   INTEGER NOT NULL,
                    total NUMERIC(20,2) GENERATED ALWAYS AS (price * qty) STORED
                );");

            var discovery = CdcSinkSchemaDiscovery.For("Npgsql");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            var products = schema.Tables.Single(t => t.SourceTableName == "products");
            Assert.True(products.Columns.Single(c => c.Name == "price").IsCdcCapturable);

            var total = products.Columns.Single(c => c.Name == "total");
            Assert.False(total.IsCdcCapturable);
            Assert.False(string.IsNullOrEmpty(total.UnsupportedReason));
        }
    }
}
