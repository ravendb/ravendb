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
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    public class CdcSinkSqlServerIntegrationTests : SqlAwareTestBase
    {
        public CdcSinkSqlServerIntegrationTests(ITestOutputHelper output) : base(output)
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

            // SQL Server creates the capture instance asynchronously via SQL Agent.
            // Wait for it to become available before proceeding.
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

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "mssql-cdc-test")
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

        private AddCdcSinkOperationResult AddCdcSink(IDocumentStore store, CdcSinkConfiguration config)
        {
            return store.Maintenance.Send(new AddCdcSinkOperation(config));
        }

        private async Task<T> WaitForDocumentAsync<T>(IDocumentStore store, string docId, int timeoutMs = 30_000)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<T>(docId);
                    if (doc != null)
                        return doc;
                }

                await Task.Delay(250);
            }

            return null;
        }

        private async Task<bool> WaitForDocumentDeletionAsync(IDocumentStore store, string docId, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<object>(docId);
                    if (doc == null)
                        return true;
                }

                await Task.Delay(250);
            }

            return false;
        }

        private async Task<int> WaitForDocumentCountAsync(IDocumentStore store, string collectionName, int expectedCount, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            int count = 0;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using (var session = store.OpenAsyncSession())
                {
                    count = await session.Query<dynamic>(collectionName: collectionName).CountAsync();
                    if (count >= expectedCount)
                        return count;
                }

                await Task.Delay(250);
            }

            return count;
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task InitialLoad_RootTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE products (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    price DECIMAL(10,2) NOT NULL
                )");

            ExecuteMsSql(connectionString, @"
                INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99);");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "products");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-initial-load",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Products",
                        SourceTableSchema = "dbo",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "Id" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "price", Name = "Price" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Products", expectedCount: 3, timeoutMs: 60_000);
            Assert.Equal(3, count);

            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<dynamic>("Products/1");
                Assert.NotNull(p1);
                Assert.Equal("Widget", (string)p1.Name);
                Assert.Equal(9.99m, (decimal)p1.Price);

                var p2 = await session.LoadAsync<dynamic>("Products/2");
                Assert.NotNull(p2);
                Assert.Equal("Gadget", (string)p2.Name);

                var p3 = await session.LoadAsync<dynamic>("Products/3");
                Assert.NotNull(p3);
                Assert.Equal("Doohickey", (string)p3.Name);
                Assert.Equal(29.99m, (decimal)p3.Price);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE events (
                    id INT PRIMARY KEY,
                    description NVARCHAR(200) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "events");

            ExecuteMsSql(connectionString, @"INSERT INTO events (id, description) VALUES (1, 'Initial Event');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Events",
                        SourceTableSchema = "dbo",
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "Id" },
                            new CdcColumnMapping { Column = "description", Name = "Description" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load to complete
            var initialDoc = await WaitForDocumentAsync<dynamic>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);

            // Insert a new row to be captured via CDC streaming
            ExecuteMsSql(connectionString, @"INSERT INTO events (id, description) VALUES (2, 'Streamed Event');");

            var newDoc = await WaitForDocumentAsync<dynamic>(store, "Events/2", timeoutMs: 60_000);
            Assert.NotNull(newDoc);
            Assert.Equal("Streamed Event", (string)newDoc.Description);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Update()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE notes (
                    id INT PRIMARY KEY,
                    content NVARCHAR(500) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "notes");

            ExecuteMsSql(connectionString, @"INSERT INTO notes (id, content) VALUES (1, 'Original Content');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Notes",
                        SourceTableSchema = "dbo",
                        SourceTableName = "notes",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "Id" },
                            new CdcColumnMapping { Column = "content", Name = "Content" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Notes/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Original Content", (string)doc.Content);

            // Update the row
            ExecuteMsSql(connectionString, @"UPDATE notes SET content = 'Updated Content' WHERE id = 1;");

            // Wait for the updated content to appear
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var updated = await session.LoadAsync<dynamic>("Notes/1");
                return (string)updated?.Content;
            }, "Updated Content", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Delete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE records (
                    id INT PRIMARY KEY,
                    title NVARCHAR(200) NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "records");

            ExecuteMsSql(connectionString, @"
                INSERT INTO records (id, title) VALUES (1, 'To Be Deleted');
                INSERT INTO records (id, title) VALUES (2, 'To Keep');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Records",
                        SourceTableSchema = "dbo",
                        SourceTableName = "records",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "Id" },
                            new CdcColumnMapping { Column = "title", Name = "Title" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Records", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete a row
            ExecuteMsSql(connectionString, @"DELETE FROM records WHERE id = 1;");

            var deleted = await WaitForDocumentDeletionAsync(store, "Records/1", timeoutMs: 60_000);
            Assert.True(deleted, "Document Records/1 should have been deleted after CDC DELETE");

            // Verify the other document still exists
            using (var session = store.OpenAsyncSession())
            {
                var kept = await session.LoadAsync<dynamic>("Records/2");
                Assert.NotNull(kept);
                Assert.Equal("To Keep", (string)kept.Title);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MsSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMsSql(connectionString, @"
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                )");

            ExecuteMsSql(connectionString, @"
                CREATE TABLE order_lines (
                    id INT PRIMARY KEY,
                    order_id INT NOT NULL REFERENCES orders(id),
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL
                )");

            EnableCdc(connectionString);
            EnableCdcOnTable(connectionString, "dbo", "orders");
            EnableCdcOnTable(connectionString, "dbo", "order_lines");

            ExecuteMsSql(connectionString, @"
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-embedded-array",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "dbo",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "Id" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "dbo",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "id" },
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Alice", (string)doc.CustomerName);

            // Wait for embedded lines to be populated
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null)
                    return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Equal(2, lines.Count);

                var products = lines.Select(l => (string)l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Apples", products);
                Assert.Contains("Bananas", products);
            }
        }
    }
}
