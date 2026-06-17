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
    public class CdcSinkSqlServerSchemaDiscoveryTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkSqlServerSchemaDiscoveryTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMsSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(MigrationProvider.MsSQL, connectionString, sql);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task DiscoversTablesColumnsPrimaryKeysAndForeignKeys()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE customers (
                    id   INT IDENTITY(1,1) PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    photo VARBINARY(MAX)
                );");
            ExecuteMsSql(connectionString, @"
                CREATE TABLE orders (
                    order_id    INT IDENTITY(1,1) PRIMARY KEY,
                    customer_id INT NOT NULL FOREIGN KEY REFERENCES customers(id),
                    total       DECIMAL(12,2) NOT NULL
                );");
            ExecuteMsSql(connectionString, @"
                CREATE TABLE order_items (
                    order_id INT NOT NULL FOREIGN KEY REFERENCES orders(order_id),
                    line_no  INT NOT NULL,
                    qty      INT NOT NULL,
                    PRIMARY KEY (order_id, line_no)
                );");

            var discovery = CdcSinkSchemaDiscovery.For("Microsoft.Data.SqlClient");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            var customers = schema.Tables.Single(t => t.SourceTableName == "customers");
            Assert.Equal("dbo", customers.SourceTableSchema);
            Assert.Equal(new[] { "id" }, customers.PrimaryKeyColumns);
            Assert.True(customers.Columns.Single(c => c.Name == "id").IsPrimaryKey);

            var photo = customers.Columns.Single(c => c.Name == "photo");
            Assert.Equal("varbinary", photo.NativeType);
            Assert.Equal(CdcColumnType.Attachment, photo.SuggestedType);

            var orders = schema.Tables.Single(t => t.SourceTableName == "orders");
            var ordersFk = Assert.Single(orders.ForeignKeys);
            Assert.Equal(new[] { "customer_id" }, ordersFk.Columns);
            Assert.Equal("customers", ordersFk.ReferencedTable);
            Assert.Equal(new[] { "id" }, ordersFk.ReferencedColumns);

            var items = schema.Tables.Single(t => t.SourceTableName == "order_items");
            Assert.Equal(new[] { "order_id", "line_no" }, items.PrimaryKeyColumns);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task FlagsCdcEnrollmentPerTableAndPerColumn()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE tracked (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100), secret VARCHAR(100));
                CREATE TABLE untracked (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100));");

            // Enable CDC at the database level then on `tracked` with a column allow-list.
            // `untracked` stays out of cdc.change_tables.
            ExecuteMsSql(connectionString, "EXEC sys.sp_cdc_enable_db");
            ExecuteMsSql(connectionString, @"
                EXEC sys.sp_cdc_enable_table
                    @source_schema   = N'dbo',
                    @source_name     = N'tracked',
                    @role_name       = NULL,
                    @captured_column_list = N'id, name'");

            var discovery = CdcSinkSchemaDiscovery.For("Microsoft.Data.SqlClient");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            var tracked = schema.Tables.Single(t => t.SourceTableName == "tracked");
            Assert.True(tracked.IsCdcEnabled);
            Assert.True(tracked.Columns.Single(c => c.Name == "id").IsCdcCapturable);
            Assert.True(tracked.Columns.Single(c => c.Name == "name").IsCdcCapturable);

            var secret = tracked.Columns.Single(c => c.Name == "secret");
            Assert.False(secret.IsCdcCapturable);
            Assert.False(string.IsNullOrEmpty(secret.UnsupportedReason));

            var untracked = schema.Tables.Single(t => t.SourceTableName == "untracked");
            Assert.False(untracked.IsCdcEnabled);
            Assert.All(untracked.Columns, c =>
            {
                Assert.False(c.IsCdcCapturable);
                Assert.False(string.IsNullOrEmpty(c.UnsupportedReason));
            });
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task RespectsSchemasFilter()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, "CREATE SCHEMA sales;");
            ExecuteMsSql(connectionString, "CREATE TABLE sales.items (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100));");
            ExecuteMsSql(connectionString, "CREATE TABLE dbo.unrelated (id INT IDENTITY(1,1) PRIMARY KEY, junk VARCHAR(100));");

            var discovery = CdcSinkSchemaDiscovery.For("Microsoft.Data.SqlClient");

            var salesOnly = await discovery.DiscoverAsync(connectionString, new[] { "sales" }, CancellationToken.None);
            Assert.Single(salesOnly.Tables);
            Assert.Equal("items", salesOnly.Tables[0].SourceTableName);

            // No schemas hint => defaults to dbo (the SQL Server CDC/Studio default), not the login's actual default schema.
            var defaultOnly = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);
            Assert.Single(defaultOnly.Tables);
            Assert.Equal("unrelated", defaultOnly.Tables[0].SourceTableName);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlCdcRequired = true)]
        public async Task ExcludesCdcAndSystemSchemaTablesFromDiscovery()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, "CREATE TABLE tracked (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100));");

            // Enabling CDC creates the internal cdc.* catalog (cdc.change_tables, cdc.lsn_time_mapping,
            // cdc.dbo_tracked_CT, ...) plus dbo.systranschemas. None of those are valid CDC sources.
            ExecuteMsSql(connectionString, "EXEC sys.sp_cdc_enable_db");
            ExecuteMsSql(connectionString, @"
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name   = N'tracked',
                    @role_name     = NULL");

            var discovery = CdcSinkSchemaDiscovery.For("Microsoft.Data.SqlClient");
            var schema = await discovery.DiscoverAsync(connectionString, schemas: null, CancellationToken.None);

            Assert.DoesNotContain(schema.Tables, t => string.Equals(t.SourceTableSchema, "cdc", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(schema.Tables, t => string.Equals(t.SourceTableSchema, "sys", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(schema.Tables, t => string.Equals(t.SourceTableName, "systranschemas", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(schema.Tables, t => string.Equals(t.SourceTableName, "tracked", StringComparison.OrdinalIgnoreCase));
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true)]
        public async Task TwoSchemasWithIdenticalConstraintNames_DoNotCollide()
        {
            // Regression for RavenDB-26636: the PK join and FK cache used to be keyed by the bare
            // CONSTRAINT_NAME. SQL Server constraint names are unique only within a schema, so two
            // schemas each carrying a 'pk_shared'/'fk_shared' pair collapsed onto one cache entry,
            // cross-wiring a child's foreign key to the wrong parent. The fix keys both by
            // (schema, constraint name); discovery must keep the two pairs distinct.
            using var teardown = WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, "CREATE SCHEMA s1;");
            ExecuteMsSql(connectionString, "CREATE SCHEMA s2;");
            ExecuteMsSql(connectionString, "CREATE TABLE s1.parent_a (id_a INT NOT NULL, CONSTRAINT pk_shared PRIMARY KEY (id_a));");
            ExecuteMsSql(connectionString, @"
                CREATE TABLE s1.child_a (cid INT PRIMARY KEY, ref_a INT NOT NULL,
                    CONSTRAINT fk_shared FOREIGN KEY (ref_a) REFERENCES s1.parent_a(id_a));");
            ExecuteMsSql(connectionString, "CREATE TABLE s2.parent_b (id_b INT NOT NULL, CONSTRAINT pk_shared PRIMARY KEY (id_b));");
            ExecuteMsSql(connectionString, @"
                CREATE TABLE s2.child_b (cid INT PRIMARY KEY, ref_b INT NOT NULL,
                    CONSTRAINT fk_shared FOREIGN KEY (ref_b) REFERENCES s2.parent_b(id_b));");

            var discovery = CdcSinkSchemaDiscovery.For("Microsoft.Data.SqlClient");
            var schema = await discovery.DiscoverAsync(connectionString, new[] { "s1", "s2" }, CancellationToken.None);

            // PK join keyed by (schema, constraint): each parent keeps its own PK column.
            var parentA = schema.Tables.Single(t => t.SourceTableSchema == "s1" && t.SourceTableName == "parent_a");
            Assert.Equal(new[] { "id_a" }, parentA.PrimaryKeyColumns);
            var parentB = schema.Tables.Single(t => t.SourceTableSchema == "s2" && t.SourceTableName == "parent_b");
            Assert.Equal(new[] { "id_b" }, parentB.PrimaryKeyColumns);

            // FK cache keyed by (schema, constraint): each child resolves to the parent in its own schema.
            var childA = schema.Tables.Single(t => t.SourceTableSchema == "s1" && t.SourceTableName == "child_a");
            var fkA = Assert.Single(childA.ForeignKeys);
            Assert.Equal(new[] { "ref_a" }, fkA.Columns);
            Assert.Equal("s1", fkA.ReferencedSchema);
            Assert.Equal("parent_a", fkA.ReferencedTable);
            Assert.Equal(new[] { "id_a" }, fkA.ReferencedColumns);

            var childB = schema.Tables.Single(t => t.SourceTableSchema == "s2" && t.SourceTableName == "child_b");
            var fkB = Assert.Single(childB.ForeignKeys);
            Assert.Equal(new[] { "ref_b" }, fkB.Columns);
            Assert.Equal("s2", fkB.ReferencedSchema);
            Assert.Equal("parent_b", fkB.ReferencedTable);
            Assert.Equal(new[] { "id_b" }, fkB.ReferencedColumns);
        }
    }
}
