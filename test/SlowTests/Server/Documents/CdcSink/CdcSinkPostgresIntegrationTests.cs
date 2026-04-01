using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
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
    public class CdcSinkPostgresIntegrationTests : SqlAwareTestBase
    {
        public CdcSinkPostgresIntegrationTests(ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteNpgSql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, connectionString, sql);
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "pg-cdc-test")
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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task InitialLoad_RootTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price NUMERIC(10,2) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-initial-load",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" },
                            { "price", "Price" }
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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task InitialLoad_WithColumnMapping()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (
                    product_id SERIAL PRIMARY KEY,
                    product_name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO items (product_id, product_name) VALUES (1, 'Alpha');
                INSERT INTO items (product_id, product_name) VALUES (2, 'Beta');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-column-mapping",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "product_id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "product_id", "Id" },
                            { "product_name", "Name" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Items", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<dynamic>("Items/1");
                Assert.NotNull(item);
                Assert.Equal("Alpha", (string)item.Name);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE events (
                    id SERIAL PRIMARY KEY,
                    description VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO events (id, description) VALUES (1, 'Initial Event');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-cdc-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Events",
                        SourceTableSchema = "public",
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "description", "Description" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load to complete
            var initialDoc = await WaitForDocumentAsync<dynamic>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);

            // Insert a new row via CDC streaming
            ExecuteNpgSql(connectionString, @"INSERT INTO events (id, description) VALUES (2, 'Streamed Event');");

            var newDoc = await WaitForDocumentAsync<dynamic>(store, "Events/2", timeoutMs: 60_000);
            Assert.NotNull(newDoc);
            Assert.Equal("Streamed Event", (string)newDoc.Description);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task CdcStreaming_Update()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE notes (
                    id SERIAL PRIMARY KEY,
                    content VARCHAR(500) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO notes (id, content) VALUES (1, 'Original Content');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-cdc-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Notes",
                        SourceTableSchema = "public",
                        SourceTableName = "notes",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "content", "Content" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Notes/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Original Content", (string)doc.Content);

            // Update the row
            ExecuteNpgSql(connectionString, @"UPDATE notes SET content = 'Updated Content' WHERE id = 1;");

            // Wait for the updated content to appear
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var updated = await session.LoadAsync<dynamic>("Notes/1");
                return (string)updated?.Content;
            }, "Updated Content", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task CdcStreaming_Delete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            // REPLICA IDENTITY FULL is required for DELETE to send full row data
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE records (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"ALTER TABLE records REPLICA IDENTITY FULL;");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO records (id, title) VALUES (1, 'To Be Deleted');
                INSERT INTO records (id, title) VALUES (2, 'To Keep');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-cdc-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Records",
                        SourceTableSchema = "public",
                        SourceTableName = "records",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "title", "Title" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Records", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete a row
            ExecuteNpgSql(connectionString, @"DELETE FROM records WHERE id = 1;");

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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE order_lines (
                    id SERIAL PRIMARY KEY,
                    order_id INT NOT NULL REFERENCES orders(id),
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-array",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "id" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "product", "Product" },
                                    { "quantity", "Quantity" }
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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchWithDollarRow()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE people (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO people (id, first_name, last_name) VALUES (1, 'John', 'Doe');
                INSERT INTO people (id, first_name, last_name) VALUES (2, 'Jane', 'Smith');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-patch-dollar-row",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "People",
                        SourceTableSchema = "public",
                        SourceTableName = "people",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" }
                        },
                        Patch = "this.FullName = $row.first_name + ' ' + $row.last_name;"
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "People", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<dynamic>("People/1");
                Assert.NotNull(p1);
                Assert.Equal("John Doe", (string)p1.FullName);

                var p2 = await session.LoadAsync<dynamic>("People/2");
                Assert.NotNull(p2);
                Assert.Equal("Jane Smith", (string)p2.FullName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task LinkedTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE customers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_id INT NOT NULL REFERENCES customers(id),
                    total NUMERIC(10,2) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO customers (id, name) VALUES (42, 'Big Corp');
                INSERT INTO orders (id, customer_id, total) VALUES (1, 42, 150.00);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-linked-table",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "total", "Total" }
                        },
                        LinkedTables = new List<CdcSinkLinkedTableConfig>
                        {
                            new CdcSinkLinkedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "customers",
                                PropertyName = "Customer",
                                LinkedCollectionName = "Customers",
                                Type = CdcSinkRelationType.Value,
                                JoinColumns = new List<string> { "customer_id" }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal(150.00m, (decimal)doc.Total);
            Assert.Equal("Customers/42", (string)doc.Customer);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray_CdcStreaming_Insert()
        {
            // Verify that CDC streaming (not just initial load) works for embedded tables
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                )");

            // Seed only the parent row
            ExecuteNpgSql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-cdc-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" },
                                    { "quantity", "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load of the parent
            var doc = await WaitForDocumentAsync<dynamic>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Now INSERT embedded rows via CDC streaming (after replication is active)
            ExecuteNpgSql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Single(lines);
                Assert.Equal("Apples", (string)lines[0].Product);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray_Delete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            // Use composite PK (order_id, line_num) so that PostgreSQL's default REPLICA IDENTITY
            // (which only sends PK columns on DELETE) includes the join column order_id.
            // Without this, a KeyDeleteMessage would only carry the PK and the CDC processor
            // couldn't route the delete to the correct parent document.
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3);
                INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 3, 'Cherries', 7);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" },
                                    { "quantity", "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for all 3 embedded lines
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 3, timeout: 60_000);

            // Delete one embedded row via CDC streaming
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2;");

            // Wait for the array to shrink from 3 to 2
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Equal(2, lines.Count);
                var products = lines.Select(l => (string)l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Apples", products);
                Assert.Contains("Cherries", products);
                Assert.DoesNotContain("Bananas", products);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray_Update()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" },
                                    { "quantity", "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            // Update an embedded row
            ExecuteNpgSql(connectionString, "UPDATE order_lines SET quantity = 99, product = 'Bananas (Updated)' WHERE order_id = 1 AND line_num = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return null;
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                var line = lines.FirstOrDefault(l => (int)l.LineNum == 2);
                return (string)line?.Product;
            }, "Bananas (Updated)", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Equal(2, lines.Count);
                var updatedLine = lines.First(l => (int)l.LineNum == 2);
                Assert.Equal(99, (int)updatedLine.Quantity);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true, Skip =
            "NavigateToEmbeddedParent does not handle array-type intermediate path segments. " +
            "When employees (3rd level) are loaded, the code navigates through Departments " +
            "(an array) but expects a BlittableJsonReaderObject. It replaces the Departments " +
            "array with an empty object, losing all department data. Fix requires matching " +
            "the correct array element using join column values during path navigation.")]
        public async Task ThreeWayNesting()
        {
            // Company → Department → Employee (3 levels deep)
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE companies (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL);
                CREATE TABLE departments (
                    company_id INT NOT NULL REFERENCES companies(id),
                    dept_id INT NOT NULL,
                    dept_name VARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id)
                );
                CREATE TABLE employees (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    emp_id INT NOT NULL,
                    emp_name VARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id, emp_id),
                    FOREIGN KEY (company_id, dept_id) REFERENCES departments(company_id, dept_id)
                );
                INSERT INTO companies (id, name) VALUES (1, 'Acme Corp');
                INSERT INTO departments (company_id, dept_id, dept_name) VALUES (1, 10, 'Engineering');
                INSERT INTO departments (company_id, dept_id, dept_name) VALUES (1, 20, 'Sales');
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 1, 'Alice');
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 2, 'Bob');
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 3, 'Charlie');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-3-way-nesting",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Companies",
                        SourceTableSchema = "public",
                        SourceTableName = "companies",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "departments",
                                PropertyName = "Departments",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "company_id" },
                                PrimaryKeyColumns = new List<string> { "dept_id" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "dept_id", "DeptId" },
                                    { "dept_name", "DeptName" }
                                },
                                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                                {
                                    new CdcSinkEmbeddedTableConfig
                                    {
                                        SourceTableSchema = "public",
                                        SourceTableName = "employees",
                                        PropertyName = "Employees",
                                        Type = CdcSinkRelationType.Array,
                                        JoinColumns = new List<string> { "dept_id" },
                                        PrimaryKeyColumns = new List<string> { "emp_id" },
                                        ColumnsMapping = new Dictionary<string, string>
                                        {
                                            { "emp_id", "EmpId" },
                                            { "emp_name", "EmpName" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Companies/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Acme Corp", (string)doc.Name);

            // Wait for departments to be populated
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<dynamic>("Companies/1");
                if (company?.Departments == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)company.Departments);
            }, 2, timeout: 60_000);

            // Verify the nested structure
            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<dynamic>("Companies/1");
                var depts = ((IEnumerable<dynamic>)company.Departments).ToList();
                Assert.Equal(2, depts.Count);

                var engineering = depts.First(d => (string)d.DeptName == "Engineering");
                Assert.NotNull(engineering);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task MultipleUpdates_SameRow_SameTransaction()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE counters (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL, value INT NOT NULL);
                INSERT INTO counters (id, name, value) VALUES (1, 'hits', 0);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-multi-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Counters",
                        SourceTableSchema = "public",
                        SourceTableName = "counters",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" },
                            { "value", "Value" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Counters/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Multiple updates to the same row in a single transaction
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                UPDATE counters SET value = 1 WHERE id = 1;
                UPDATE counters SET value = 2 WHERE id = 1;
                UPDATE counters SET value = 3 WHERE id = 1;
                COMMIT;");

            // The last update should win
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var counter = await session.LoadAsync<dynamic>("Counters/1");
                return (int?)counter?.Value;
            }, 3, timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task Transaction_InsertUpdateDeleteInsert_SameRow()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(200) NOT NULL);
                ALTER TABLE items REPLICA IDENTITY FULL;");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-insert-delete-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load to complete (empty table — just wait for process to start)
            await Task.Delay(3000);

            // In a single transaction: insert, update, delete, then re-insert the same row
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                INSERT INTO items (id, name) VALUES (1, 'First');
                UPDATE items SET name = 'Second' WHERE id = 1;
                DELETE FROM items WHERE id = 1;
                INSERT INTO items (id, name) VALUES (1, 'Final');
                COMMIT;");

            // The final state should be the last insert
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var item = await session.LoadAsync<dynamic>("Items/1");
                return (string)item?.Name;
            }, "Final", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task Transaction_MultipleDistinctRootDocuments()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL, price NUMERIC(10,2) NOT NULL);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-multi-root",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" },
                            { "price", "Price" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await Task.Delay(3000);

            // Single transaction creates multiple distinct documents
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99);
                COMMIT;");

            var count = await WaitForDocumentCountAsync(store, "Products", expectedCount: 3, timeoutMs: 60_000);
            Assert.Equal(3, count);

            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<dynamic>("Products/1");
                Assert.Equal("Widget", (string)p1.Name);
                var p3 = await session.LoadAsync<dynamic>("Products/3");
                Assert.Equal("Doohickey", (string)p3.Name);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task Transaction_MultipleRootAndEmbedded()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-and-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await Task.Delay(3000);

            // Single transaction: create parent + embedded children for two different orders
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO orders (id, customer_name) VALUES (2, 'Bob');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (2, 1, 'Cherries');
                COMMIT;");

            // Wait for both orders
            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Wait for embedded lines on order 1
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<dynamic>("Orders/2");
                Assert.Equal("Bob", (string)order2.CustomerName);
                var lines2 = ((IEnumerable<dynamic>)order2.Lines).ToList();
                Assert.Single(lines2);
                Assert.Equal("Cherries", (string)lines2[0].Product);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PropertyRetention_OnUpdate()
        {
            // Verify that fields set directly in RavenDB are preserved when a CDC update arrives
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL, email VARCHAR(200));
                INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-property-retention",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Customers",
                        SourceTableSchema = "public",
                        SourceTableName = "customers",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" },
                            { "email", "Email" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Customers/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Add a RavenDB-only field directly
            using (var session = store.OpenAsyncSession())
            {
                var customer = await session.LoadAsync<dynamic>("Customers/1");
                customer.InternalNotes = "VIP customer";
                await session.SaveChangesAsync();
            }

            // Now update the row in PostgreSQL
            ExecuteNpgSql(connectionString, "UPDATE customers SET name = 'Alice Updated' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var customer = await session.LoadAsync<dynamic>("Customers/1");
                return (string)customer?.Name;
            }, "Alice Updated", timeout: 60_000);

            // Verify the RavenDB-only field is preserved
            using (var session = store.OpenAsyncSession())
            {
                var customer = await session.LoadAsync<dynamic>("Customers/1");
                Assert.Equal("Alice Updated", (string)customer.Name);
                Assert.Equal("VIP customer", (string)customer.InternalNotes);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task UpdateParentAndEmbeddedTogether()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-update-parent-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 1, timeout: 60_000);

            // Update both parent and embedded in the same transaction
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                UPDATE orders SET customer_name = 'Alice Updated' WHERE id = 1;
                UPDATE order_lines SET product = 'Oranges' WHERE order_id = 1 AND line_num = 1;
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Grapes');
                COMMIT;");

            // Wait for both changes
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                return (string)order?.CustomerName;
            }, "Alice Updated", timeout: 60_000);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                var products = lines.Select(l => (string)l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Oranges", products);
                Assert.Contains("Grapes", products);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task MetadataExpires_ViaPatch()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE events (id SERIAL PRIMARY KEY, title VARCHAR(200) NOT NULL, expires_at TIMESTAMP);
                INSERT INTO events (id, title, expires_at) VALUES (1, 'Flash Sale', '2099-12-31 23:59:59');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-expires-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Events",
                        SourceTableSchema = "public",
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "title", "Title" }
                        },
                        Patch = @"
                            if ($row.expires_at) {
                                this['@metadata'] = this['@metadata'] || {};
                                this['@metadata']['@expires'] = $row.expires_at;
                            }"
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Flash Sale", (string)doc.Title);

            using (var session = store.OpenAsyncSession())
            {
                var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<dynamic>("Events/1"));
                Assert.True(metadata.ContainsKey("@expires"), "Document should have @expires metadata set by the patch script");
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task BinaryColumn_RootAttachment()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE files (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL, content BYTEA);
                INSERT INTO files (id, name, content) VALUES (1, 'readme.txt', decode('48656C6C6F20576F726C64', 'hex'));");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-binary-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Files",
                        SourceTableSchema = "public",
                        SourceTableName = "files",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        },
                        AttachmentNameMapping = new Dictionary<string, string>
                        {
                            { "content", "file" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Files/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("readme.txt", (string)doc.Name);

            // Verify attachment exists
            using (var session = store.OpenAsyncSession())
            {
                var file = await session.LoadAsync<object>("Files/1");
                var attachments = session.Advanced.Attachments.GetNames(file);
                Assert.True(attachments.Length > 0, "Expected at least one attachment (binary column mapped to 'file')");
                Assert.Contains("file", attachments.Select(a => a.Name));
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchScript_CombinedRootAndEmbedded()
        {
            // Root patch computes a derived field; embedded patch runs on child rows
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE invoices (id SERIAL PRIMARY KEY, customer VARCHAR(200) NOT NULL, discount_pct NUMERIC(5,2) DEFAULT 0);
                CREATE TABLE invoice_lines (
                    invoice_id INT NOT NULL REFERENCES invoices(id),
                    line_num INT NOT NULL,
                    description VARCHAR(200) NOT NULL,
                    amount NUMERIC(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num)
                );
                INSERT INTO invoices (id, customer, discount_pct) VALUES (1, 'Big Corp', 10.00);
                INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00);
                INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-combined-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Invoices",
                        SourceTableSchema = "public",
                        SourceTableName = "invoices",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer", "Customer" }
                        },
                        Patch = "this.DiscountPct = $row.discount_pct;",
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "invoice_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "invoice_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "description", "Description" }
                                },
                                Patch = "this.LineAmount = $row.amount;"
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Invoices/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Big Corp", (string)doc.Customer);
            Assert.Equal(10.00, (double)doc.DiscountPct, 2);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<dynamic>("Invoices/1");
                if (inv?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)inv.Lines);
            }, 2, timeout: 60_000);

            // Embedded patch sets this.LineAmount on the root document (this = the document).
            // The last embedded row's patch wins, so LineAmount = 200.00 (from line_num=2).
            using (var session = store.OpenAsyncSession())
            {
                var inv = await session.LoadAsync<dynamic>("Invoices/1");
                Assert.Equal(200.00, (double)inv.LineAmount, 2);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray_AddAndRemoveInSameTransaction()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-add-remove-txn",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 2, timeout: 60_000);

            // In a single transaction: add a new line and remove an existing one
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1;
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries');
                COMMIT;");

            // Should end up with 2 lines: Bananas (2) and Cherries (3) — Apples (1) deleted
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return false;
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                var products = lines.Select(l => (string)l.Product).OrderBy(p => p).ToList();
                return products.Contains("Cherries") && !products.Contains("Apples");
            }, true, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Equal(2, lines.Count);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ChildBeforeParent()
        {
            // Insert the embedded child row before the parent row exists.
            // The CDC processor should create a stub document for the parent, then
            // the parent insert fills in the root fields.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            // Create tables without FK constraint so we can insert child before parent
            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (id INT PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-child-before-parent",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "customer_name", "CustomerName" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                PrimaryKeyColumns = new List<string> { "line_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "line_num", "LineNum" },
                                    { "product", "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await Task.Delay(3000);

            // Insert child row FIRST (no parent yet in the CDC stream)
            ExecuteNpgSql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');");

            // A stub document should be created with the embedded line
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                if (order?.Lines == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)order.Lines);
            }, 1, timeout: 60_000);

            // Now insert the parent
            ExecuteNpgSql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<dynamic>("Orders/1");
                return (string)order?.CustomerName;
            }, "Alice", timeout: 60_000);

            // Both the parent fields and embedded lines should be present
            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<dynamic>("Orders/1");
                Assert.Equal("Alice", (string)order.CustomerName);
                var lines = ((IEnumerable<dynamic>)order.Lines).ToList();
                Assert.Single(lines);
                Assert.Equal("Apples", (string)lines[0].Product);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchScript_ModifiesMappedData()
        {
            // Patch script reads unmapped columns from $row and modifies mapped columns
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    base_price NUMERIC(10,2) NOT NULL,
                    tax_rate NUMERIC(5,2) NOT NULL DEFAULT 0
                );
                INSERT INTO products (id, name, base_price, tax_rate) VALUES (1, 'Widget', 100.00, 0.20);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-patch-modifies",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        },
                        // Patch reads base_price and tax_rate (unmapped) and computes TotalPrice
                        Patch = "this.TotalPrice = $row.base_price * (1 + $row.tax_rate);"
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<dynamic>(store, "Products/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Widget", (string)doc.Name);
            // 100.00 * (1 + 0.20) = 120.00
            Assert.Equal(120.00, (double)doc.TotalPrice, 2);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task BinaryColumn_EmbeddedAttachment()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE albums (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL);
                CREATE TABLE photos (
                    album_id INT NOT NULL REFERENCES albums(id),
                    photo_num INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    thumbnail BYTEA,
                    PRIMARY KEY (album_id, photo_num)
                );
                INSERT INTO albums (id, name) VALUES (1, 'Vacation');
                INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', decode('89504E47', 'hex'));
                INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', decode('FFD8FFE0', 'hex'));");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Albums",
                        SourceTableSchema = "public",
                        SourceTableName = "albums",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "photos",
                                PropertyName = "Photos",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "album_id" },
                                PrimaryKeyColumns = new List<string> { "photo_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "photo_num", "PhotoNum" },
                                    { "title", "Title" }
                                },
                                AttachmentNameMapping = new Dictionary<string, string>
                                {
                                    { "thumbnail", "thumb" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var album = await session.LoadAsync<dynamic>("Albums/1");
                if (album?.Photos == null) return 0;
                return (int)Enumerable.Count((IEnumerable<dynamic>)album.Photos);
            }, 2, timeout: 60_000);

            // Verify embedded attachments exist with prefixed names
            using (var session = store.OpenAsyncSession())
            {
                var album = await session.LoadAsync<object>("Albums/1");
                var attachments = session.Advanced.Attachments.GetNames(album);
                // Each embedded photo should have an attachment named "Photos/{photo_num}/thumb"
                Assert.True(attachments.Length >= 2, $"Expected at least 2 attachments, got {attachments.Length}");
                var names = attachments.Select(a => a.Name).ToList();
                Assert.Contains("Photos/1/thumb", names);
                Assert.Contains("Photos/2/thumb", names);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task DeleteAttachment_OnEmbeddedDelete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE albums (id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL);
                CREATE TABLE photos (
                    album_id INT NOT NULL REFERENCES albums(id),
                    photo_num INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    thumbnail BYTEA,
                    PRIMARY KEY (album_id, photo_num)
                );
                INSERT INTO albums (id, name) VALUES (1, 'Vacation');
                INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', decode('89504E47', 'hex'));
                INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', decode('FFD8FFE0', 'hex'));");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-delete-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        Name = "Albums",
                        SourceTableSchema = "public",
                        SourceTableName = "albums",
                        PrimaryKeyColumns = new List<string> { "id" },
                        ColumnsMapping = new Dictionary<string, string>
                        {
                            { "id", "Id" },
                            { "name", "Name" }
                        },
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "photos",
                                PropertyName = "Photos",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "album_id" },
                                PrimaryKeyColumns = new List<string> { "photo_num" },
                                ColumnsMapping = new Dictionary<string, string>
                                {
                                    { "photo_num", "PhotoNum" },
                                    { "title", "Title" }
                                },
                                AttachmentNameMapping = new Dictionary<string, string>
                                {
                                    { "thumbnail", "thumb" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for both photos + attachments
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var album = await session.LoadAsync<object>("Albums/1");
                if (album == null) return 0;
                return session.Advanced.Attachments.GetNames(album).Length;
            }, 2, timeout: 60_000);

            // Delete one photo — its attachment should also be removed
            ExecuteNpgSql(connectionString, "DELETE FROM photos WHERE album_id = 1 AND photo_num = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var album = await session.LoadAsync<object>("Albums/1");
                if (album == null) return -1;
                return session.Advanced.Attachments.GetNames(album).Length;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var album = await session.LoadAsync<object>("Albums/1");
                var attachments = session.Advanced.Attachments.GetNames(album);
                Assert.Single(attachments);
                Assert.Equal("Photos/2/thumb", attachments[0].Name);
            }
        }
    }
}
