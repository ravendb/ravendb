using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    /// <summary>
    /// Exercises <see cref="GetCdcSinkSchemaOperation"/> end-to-end: client builds the
    /// request, hits POST /admin/cdc-sink/schema, deserializes the response into the
    /// typed <see cref="CdcSinkSourceSchema"/> graph. Complements the direct
    /// <c>CdcSinkSchemaDiscovery.For(...).DiscoverAsync(...)</c> tests in
    /// <see cref="CdcSinkPostgresSchemaDiscoveryTests"/>.
    /// </summary>
    public class CdcSinkPostgresSchemaClientApiTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresSchemaClientApiTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.NpgSQL, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_HappyPath_RoundTripsTypedDtos()
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
                    total        NUMERIC(12,2) NOT NULL
                );
            ");

            using var store = GetDocumentStore();

            var connection = new SqlConnectionString
            {
                FactoryName = "Npgsql",
                ConnectionString = connectionString,
            };

            var schema = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(connection));

            Assert.NotNull(schema);
            Assert.NotNull(schema.CatalogName);
            Assert.Equal(2, schema.Tables.Count);

            var customers = schema.Tables.Single(t => t.SourceTableName == "customers");
            Assert.Equal("public", customers.SourceTableSchema);
            Assert.True(customers.IsCdcEnabled);
            Assert.Equal(new[] { "id" }, customers.PrimaryKeyColumns);

            var metadata = customers.Columns.Single(c => c.Name == "metadata");
            Assert.Equal("jsonb", metadata.NativeType);
            Assert.Equal(CdcColumnType.Json, metadata.SuggestedType);

            var photo = customers.Columns.Single(c => c.Name == "photo");
            Assert.Equal("bytea", photo.NativeType);
            Assert.Equal(CdcColumnType.Attachment, photo.SuggestedType);

            var orders = schema.Tables.Single(t => t.SourceTableName == "orders");
            var fk = Assert.Single(orders.ForeignKeys);
            Assert.Equal(new[] { "customer_id" }, fk.Columns);
            Assert.Equal("customers", fk.ReferencedTable);
            Assert.Equal(new[] { "id" }, fk.ReferencedColumns);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_RespectsSchemasFilter()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE SCHEMA shop;
                CREATE TABLE shop.items (id SERIAL PRIMARY KEY, name VARCHAR(100));
                CREATE TABLE public.unrelated (id SERIAL PRIMARY KEY, junk VARCHAR(100));
            ");

            using var store = GetDocumentStore();

            var connection = new SqlConnectionString
            {
                FactoryName = "Npgsql",
                ConnectionString = connectionString,
            };

            var shopOnly = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(connection, new[] { "shop" }));
            Assert.Single(shopOnly.Tables);
            Assert.Equal("items", shopOnly.Tables[0].SourceTableName);

            var publicOnly = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(connection));
            Assert.Single(publicOnly.Tables);
            Assert.Equal("unrelated", publicOnly.Tables[0].SourceTableName);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_ConnectionFailure_ReturnsErrorsInsteadOf500()
        {
            using var store = GetDocumentStore();

            // Point at a host that won't resolve. The driver will throw inside DiscoverAsync;
            // the handler must catch it and return a structured response with Errors populated.
            var connection = new SqlConnectionString
            {
                FactoryName = "Npgsql",
                ConnectionString = "Host=cdc-test-no-such-host.invalid;Database=postgres;User Id=postgres;Password=x;Timeout=2;Command Timeout=2",
            };

            var result = await store.Maintenance.SendAsync(new GetCdcSinkSchemaOperation(connection));

            Assert.NotNull(result);
            Assert.Empty(result.Tables);
            var error = Assert.Single(result.Errors);
            // Generic operator-friendly message; must NOT echo the raw driver text (no host name in the response).
            Assert.Contains("Schema discovery against the source database failed", error);
            Assert.DoesNotContain("cdc-test-no-such-host", error);
        }
    }
}
