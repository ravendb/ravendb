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
    }
}
