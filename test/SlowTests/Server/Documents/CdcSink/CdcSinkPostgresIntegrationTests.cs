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
    /// <summary>
    /// PostgreSQL replication slots are cluster-global — tests that create/drop slots
    /// and publications can interfere when running in parallel. This collection ensures
    /// CDC Sink Postgres tests run sequentially.
    /// </summary>
    [CollectionDefinition(nameof(CdcSinkPostgresTests), DisableParallelization = true)]
    public class CdcSinkPostgresTests;

    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSinkPostgresIntegrationTests : CdcSinkIntegrationTestBase
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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task InitialLoad_RootTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price NUMERIC(12,2) NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99);
                INSERT INTO products (id, name, price) VALUES (4, 'Precision', 123456789.01);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-initial-load",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "price", Name = "Price" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Products", expectedCount: 4, timeoutMs: 60_000);
            Assert.Equal(4, count);

            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<Product>("Products/1");
                Assert.NotNull(p1);
                Assert.Equal("Widget", p1.Name);
                Assert.Equal(9.99m, p1.Price);

                var p2 = await session.LoadAsync<Product>("Products/2");
                Assert.NotNull(p2);
                Assert.Equal("Gadget", p2.Name);

                var p3 = await session.LoadAsync<Product>("Products/3");
                Assert.NotNull(p3);
                Assert.Equal("Doohickey", p3.Name);
                Assert.Equal(29.99m, p3.Price);

                // Precision-sensitive value: 123456789.01 cannot survive a decimal→double→decimal round-trip
                var p4 = await session.LoadAsync<Product>("Products/4");
                Assert.NotNull(p4);
                Assert.Equal(123456789.01m, p4.Price);
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
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "product_id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "product_id", Name = "DbId" },
                            new CdcColumnMapping { Column = "product_name", Name = "Name" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Items", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<Item>("Items/1");
                Assert.NotNull(item);
                Assert.Equal("Alpha", item.Name);
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
                        CollectionName = "Events",
                        SourceTableSchema = "public",
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "description", Name = "Description" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load to complete
            var initialDoc = await WaitForDocumentAsync<Event>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);

            // Insert a new row via CDC streaming
            ExecuteNpgSql(connectionString, @"INSERT INTO events (id, description) VALUES (2, 'Streamed Event');");

            var newDoc = await WaitForDocumentAsync<Event>(store, "Events/2", timeoutMs: 60_000);
            Assert.NotNull(newDoc);
            Assert.Equal("Streamed Event", newDoc.Description);
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
                        CollectionName = "Notes",
                        SourceTableSchema = "public",
                        SourceTableName = "notes",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "content", Name = "Content" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Note>(store, "Notes/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Original Content", doc.Content);

            // Update the row
            ExecuteNpgSql(connectionString, @"UPDATE notes SET content = 'Updated Content' WHERE id = 1;");

            // Wait for the updated content to appear
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var updated = await session.LoadAsync<Note>("Notes/1");
                return updated?.Content;
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
                        CollectionName = "Records",
                        SourceTableSchema = "public",
                        SourceTableName = "records",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "title", Name = "Title" }
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
                var kept = await session.LoadAsync<Record>("Records/2");
                Assert.NotNull(kept);
                Assert.Equal("To Keep", kept.Title);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "id", Name = "LineId" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Order>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Alice", doc.CustomerName);

            // Wait for embedded lines to be populated
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null)
                    return 0;
                return order.Lines.Count;
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                Assert.Equal(2, lines.Count);

                var products = lines.Select(l => l.Product).OrderBy(p => p).ToList();
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
                        CollectionName = "People",
                        SourceTableSchema = "public",
                        SourceTableName = "people",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" }
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
                var p1 = await session.LoadAsync<Person>("People/1");
                Assert.NotNull(p1);
                Assert.Equal("John Doe", p1.FullName);

                var p2 = await session.LoadAsync<Person>("People/2");
                Assert.NotNull(p2);
                Assert.Equal("Jane Smith", p2.FullName);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "total", Name = "Total" }
                        },
                        LinkedTables = new List<CdcSinkLinkedTableConfig>
                        {
                            new CdcSinkLinkedTableConfig
                            {
                                SourceTableSchema = "public",
                                SourceTableName = "customers",
                                PropertyName = "Customer",
                                LinkedCollectionName = "Customers",
                                JoinColumns = new List<string> { "customer_id" }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Order>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal(150.00m, doc.Total);
            Assert.Equal("Customers/42", doc.Customer);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load of the parent
            var doc = await WaitForDocumentAsync<Order>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Now INSERT embedded rows via CDC streaming (after replication is active)
            ExecuteNpgSql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                Assert.Single(lines);
                Assert.Equal("Apples", lines[0].Product);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 3, timeout: 60_000);

            // Delete one embedded row via CDC streaming
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2;");

            // Wait for the array to shrink from 3 to 2
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                Assert.Equal(2, lines.Count);
                var products = lines.Select(l => l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Apples", products);
                Assert.Contains("Cherries", products);
                Assert.DoesNotContain("Bananas", products);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedArray_Delete_NonCompositePK()
        {
            // Embedded table has a simple auto-increment PK (id) that does NOT include
            // the join column (order_id). The CDC setup should automatically set
            // REPLICA IDENTITY FULL on the embedded table so DELETE events include
            // the join column needed for parent document routing.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                );
                CREATE TABLE order_lines (
                    id SERIAL PRIMARY KEY,
                    order_id INT NOT NULL REFERENCES orders(id),
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3);
                INSERT INTO order_lines (id, order_id, product, quantity) VALUES (3, 1, 'Cherries', 7);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-noncomposite-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                // Simple PK that does NOT include order_id —
                                // CDC setup should auto-set REPLICA IDENTITY FULL
                                PrimaryKeyColumns = new List<string> { "id" },
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "id", Name = "LineId" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
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
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 3, timeout: 60_000);

            // Delete one embedded row — should work because REPLICA IDENTITY FULL
            // was auto-set, so the DELETE event includes order_id for routing
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE id = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var products = order.Lines.Select(l => l.Product).OrderBy(p => p).ToList();
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" },
                                    new CdcColumnMapping { Column = "quantity", Name = "Quantity" }
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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 2, timeout: 60_000);

            // Update an embedded row
            ExecuteNpgSql(connectionString, "UPDATE order_lines SET quantity = 99, product = 'Bananas (Updated)' WHERE order_id = 1 AND line_num = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return null;
                var lines = order.Lines;
                var line = lines.FirstOrDefault(l => l.LineNum == 2);
                return line?.Product;
            }, "Bananas (Updated)", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                Assert.Equal(2, lines.Count);
                var updatedLine = lines.First(l => l.LineNum == 2);
                Assert.Equal(99, updatedLine.Quantity);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ThreeWayNesting()
        {
            // Company → Department → Employee (3 levels deep)
            // Employees join to departments via dept_id. Since the employees PK is a composite
            // (company_id, dept_id, emp_id), the default REPLICA IDENTITY includes dept_id,
            // so DELETE events carry enough data for routing without USING INDEX.
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
                        CollectionName = "Companies",
                        SourceTableSchema = "public",
                        SourceTableName = "companies",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "dept_id", Name = "DeptId" },
                                    new CdcColumnMapping { Column = "dept_name", Name = "DeptName" }
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
                                        Columns = new List<CdcColumnMapping>
                                        {
                                            new CdcColumnMapping { Column = "emp_id", Name = "EmpId" },
                                            new CdcColumnMapping { Column = "emp_name", Name = "EmpName" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for employees to be nested inside departments
            // Engineering has Alice (emp_id=1) and Bob (emp_id=2), Sales has Charlie (emp_id=3)
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<Company>("Companies/1");
                if (company?.Departments == null) return 0;
                int total = 0;
                foreach (var dept in company.Departments)
                    total += dept.Employees?.Count ?? 0;
                return total;
            }, 3, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("Companies/1");
                Assert.Equal("Acme Corp", company.Name);
                Assert.Equal(2, company.Departments.Count);

                var engineering = company.Departments.First(d => d.DeptName == "Engineering");
                Assert.Equal(2, engineering.Employees.Count);
                var empNames = engineering.Employees.Select(e => e.EmpName).OrderBy(n => n).ToList();
                Assert.Equal("Alice", empNames[0]);
                Assert.Equal("Bob", empNames[1]);

                var sales = company.Departments.First(d => d.DeptName == "Sales");
                Assert.Single(sales.Employees);
                Assert.Equal("Charlie", sales.Employees[0].EmpName);
            }

            // Delete one employee from Engineering (Alice, emp_id=1)
            ExecuteNpgSql(connectionString, "DELETE FROM employees WHERE company_id = 1 AND dept_id = 10 AND emp_id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<Company>("Companies/1");
                var eng = company?.Departments?.FirstOrDefault(d => d.DeptName == "Engineering");
                return eng?.Employees?.Count ?? 0;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("Companies/1");
                var engineering = company.Departments.First(d => d.DeptName == "Engineering");
                Assert.Single(engineering.Employees);
                Assert.Equal("Bob", engineering.Employees[0].EmpName);

                // Sales department should be unaffected
                var sales = company.Departments.First(d => d.DeptName == "Sales");
                Assert.Single(sales.Employees);
                Assert.Equal("Charlie", sales.Employees[0].EmpName);
            }

            // Delete entire Engineering department (dept_id=10) — first delete remaining employee, then department
            ExecuteNpgSql(connectionString, @"
                BEGIN;
                DELETE FROM employees WHERE company_id = 1 AND dept_id = 10;
                DELETE FROM departments WHERE company_id = 1 AND dept_id = 10;
                COMMIT;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<Company>("Companies/1");
                return company?.Departments?.Count ?? 0;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<Company>("Companies/1");
                Assert.Single(company.Departments);
                Assert.Equal("Sales", company.Departments[0].DeptName);
                Assert.Single(company.Departments[0].Employees);
                Assert.Equal("Charlie", company.Departments[0].Employees[0].EmpName);
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
                        CollectionName = "Counters",
                        SourceTableSchema = "public",
                        SourceTableName = "counters",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "value", Name = "Value" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Counter>(store, "Counters/1", timeoutMs: 60_000);
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
                var counter = await session.LoadAsync<Counter>("Counters/1");
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
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await WaitForCdcInitialLoadAsync(store, "test-insert-delete-insert");

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
                var item = await session.LoadAsync<Item>("Items/1");
                return item?.Name;
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
                        CollectionName = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "price", Name = "Price" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-multi-root");

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
                var p1 = await session.LoadAsync<Product>("Products/1");
                Assert.Equal("Widget", p1.Name);
                var p3 = await session.LoadAsync<Product>("Products/3");
                Assert.Equal("Doohickey", p3.Name);
            }

            // Verify documents were created in the same order as the SQL inserts
            // by comparing their change vector etags (monotonically increasing)
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<Product>("Products/1");
                var p2 = await session.LoadAsync<Product>("Products/2");
                var p3 = await session.LoadAsync<Product>("Products/3");

                var cv1 = session.Advanced.GetChangeVectorFor(p1);
                var cv2 = session.Advanced.GetChangeVectorFor(p2);
                var cv3 = session.Advanced.GetChangeVectorFor(p3);

                var etag1 = Raven.Server.Utils.ChangeVectorUtils.GetEtagById(cv1, db.DbBase64Id);
                var etag2 = Raven.Server.Utils.ChangeVectorUtils.GetEtagById(cv2, db.DbBase64Id);
                var etag3 = Raven.Server.Utils.ChangeVectorUtils.GetEtagById(cv3, db.DbBase64Id);

                Assert.True(etag1 < etag2, $"Product/1 etag ({etag1}) should be less than Product/2 etag ({etag2})");
                Assert.True(etag2 < etag3, $"Product/2 etag ({etag2}) should be less than Product/3 etag ({etag3})");
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-root-and-embedded");

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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<Order>("Orders/2");
                Assert.Equal("Bob", order2.CustomerName);
                var lines2 = order2.Lines;
                Assert.Single(lines2);
                Assert.Equal("Cherries", lines2[0].Product);
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
                        CollectionName = "Customers",
                        SourceTableSchema = "public",
                        SourceTableName = "customers",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "email", Name = "Email" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Customer>(store, "Customers/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Add a RavenDB-only field directly
            using (var session = store.OpenAsyncSession())
            {
                var customer = await session.LoadAsync<Customer>("Customers/1");
                customer.InternalNotes = "VIP customer";
                await session.SaveChangesAsync();
            }

            // Now update the row in PostgreSQL
            ExecuteNpgSql(connectionString, "UPDATE customers SET name = 'Alice Updated' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var customer = await session.LoadAsync<Customer>("Customers/1");
                return customer?.Name;
            }, "Alice Updated", timeout: 60_000);

            // Verify the RavenDB-only field is preserved
            using (var session = store.OpenAsyncSession())
            {
                var customer = await session.LoadAsync<Customer>("Customers/1");
                Assert.Equal("Alice Updated", customer.Name);
                Assert.Equal("VIP customer", customer.InternalNotes);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
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
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.CustomerName;
            }, "Alice Updated", timeout: 60_000);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 2, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                var products = lines.Select(l => l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Oranges", products);
                Assert.Contains("Grapes", products);
            }

            // Delete one line and verify removal
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                Assert.Single(order.Lines);
                Assert.Equal("Grapes", order.Lines[0].Product);
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
                        CollectionName = "Events",
                        SourceTableSchema = "public",
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "title", Name = "Title" }
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

            var doc = await WaitForDocumentAsync<Event>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Flash Sale", doc.Title);

            using (var session = store.OpenAsyncSession())
            {
                var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<Event>("Events/1"));
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
                        CollectionName = "Files",
                        SourceTableSchema = "public",
                        SourceTableName = "files",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "content", Name = "file", Type = CdcColumnType.Attachment }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<FileDoc>(store, "Files/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("readme.txt", doc.Name);

            // Verify attachment exists
            using (var session = store.OpenAsyncSession())
            {
                var file = await session.LoadAsync<object>("Files/1");
                var attachments = session.Advanced.Attachments.GetNames(file);
                Assert.True(attachments.Length > 0, "Expected at least one attachment (binary column mapped to 'file')");
                Assert.Contains("file", attachments.Select(a => a.Name));
            }

            using (var session2 = store.OpenAsyncSession())
            using (var attachmentResult = await session2.Advanced.Attachments.GetAsync("Files/1", "file"))
            {
                Assert.NotNull(attachmentResult);
                using var ms = new System.IO.MemoryStream();
                await attachmentResult.Stream.CopyToAsync(ms);
                var content = System.Text.Encoding.ASCII.GetString(ms.ToArray());
                Assert.Equal("Hello World", content);
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
                        CollectionName = "Invoices",
                        SourceTableSchema = "public",
                        SourceTableName = "invoices",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer", Name = "Customer" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "description", Name = "Description" }
                                },
                                Patch = "this.LineAmount = $row.amount;"
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Invoice>(store, "Invoices/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Big Corp", doc.Customer);
            Assert.Equal(10.00, doc.DiscountPct, 2);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<Invoice>("Invoices/1");
                if (inv?.Lines == null) return 0;
                return inv.Lines.Count;
            }, 2, timeout: 60_000);

            // Embedded patch sets this.LineAmount on the root document (this = the document).
            // The last embedded row's patch wins, so LineAmount = 200.00 (from line_num=2).
            using (var session = store.OpenAsyncSession())
            {
                var inv = await session.LoadAsync<Invoice>("Invoices/1");
                Assert.Equal(200.00, inv.LineAmount, 2);
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
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
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return false;
                var lines = order.Lines;
                var products = lines.Select(l => l.Product).OrderBy(p => p).ToList();
                return products.Contains("Cherries") && !products.Contains("Apples");
            }, true, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
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
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-child-before-parent");

            // Insert child row FIRST (no parent yet in the CDC stream)
            ExecuteNpgSql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');");

            // A stub document should be created with the embedded line
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 1, timeout: 60_000);

            // Now insert the parent
            ExecuteNpgSql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.CustomerName;
            }, "Alice", timeout: 60_000);

            // Both the parent fields and embedded lines should be present
            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                Assert.Equal("Alice", order.CustomerName);
                var lines = order.Lines;
                Assert.Single(lines);
                Assert.Equal("Apples", lines[0].Product);
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
                        CollectionName = "Products",
                        SourceTableSchema = "public",
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        // Patch reads base_price and tax_rate (unmapped) and computes TotalPrice
                        Patch = "this.TotalPrice = $row.base_price * (1 + $row.tax_rate);"
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<Product>(store, "Products/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Widget", doc.Name);
            // 100.00 * (1 + 0.20) = 120.00
            Assert.Equal(120.00, doc.TotalPrice, 2);
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
                        CollectionName = "Albums",
                        SourceTableSchema = "public",
                        SourceTableName = "albums",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "photo_num", Name = "PhotoNum" },
                                    new CdcColumnMapping { Column = "title", Name = "Title" },
                                    new CdcColumnMapping { Column = "thumbnail", Name = "thumb", Type = CdcColumnType.Attachment }
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
                var album = await session.LoadAsync<Album>("Albums/1");
                if (album?.Photos == null) return 0;
                return album.Photos.Count;
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

            using (var session2 = store.OpenAsyncSession())
            using (var attachmentResult = await session2.Advanced.Attachments.GetAsync("Albums/1", "Photos/1/thumb"))
            {
                Assert.NotNull(attachmentResult);
                using var ms = new System.IO.MemoryStream();
                await attachmentResult.Stream.CopyToAsync(ms);
                Assert.True(ms.Length > 0, "Attachment content should not be empty");
                // Verify it's the PNG header bytes we inserted: 89504E47
                Assert.Equal(0x89, ms.ToArray()[0]);
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
                        CollectionName = "Albums",
                        SourceTableSchema = "public",
                        SourceTableName = "albums",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "photo_num", Name = "PhotoNum" },
                                    new CdcColumnMapping { Column = "title", Name = "Title" },
                                    new CdcColumnMapping { Column = "thumbnail", Name = "thumb", Type = CdcColumnType.Attachment }
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

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchOnDelete_RootTable_ArchivePattern()
        {
            // Instead of deleting the document, PatchOnDelete marks it as archived.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                );
                ALTER TABLE orders REPLICA IDENTITY FULL;
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO orders (id, customer_name) VALUES (2, 'Bob');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-archive-root",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            // IgnoreDeletes prevents the document from being deleted.
                            // Patch runs to mark it as archived.
                            IgnoreDeletes = true,
                            Patch = "this.Archived = true; this.ArchivedAt = new Date().toISOString();"
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete order 1 in PostgreSQL
            ExecuteNpgSql(connectionString, "DELETE FROM orders WHERE id = 1;");

            // Document should NOT be deleted — it should be archived
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<ArchivedOrder>("Orders/1");
                return order?.Archived;
            }, true, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order1 = await session.LoadAsync<ArchivedOrder>("Orders/1");
                Assert.True(order1.Archived);
                Assert.NotNull(order1.ArchivedAt);
                Assert.Equal("Alice", order1.CustomerName);

                // Order 2 should be unaffected
                var order2 = await session.LoadAsync<ArchivedOrder>("Orders/2");
                Assert.False(order2.Archived);
                Assert.Equal("Bob", order2.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchOnDelete_EmbeddedTable()
        {
            // When an embedded row is deleted, PatchOnDelete runs on the parent doc
            // instead of removing the item from the array.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                );
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-patchondelete-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
                                },
                                // Audit the deletion on the parent — no return true, so the item IS removed
                                OnDelete = new CdcSinkOnDeleteConfig { Patch = "this.LastDeletedLine = $row.line_num; this.DeleteCount = (this.DeleteCount || 0) + 1;" }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 2, timeout: 60_000);

            // Delete one embedded row
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2;");

            // The item IS removed (no return true), but the patch ran first
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                return order?.DeleteCount;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                // Item was removed because PatchOnDelete didn't return true
                Assert.Equal(1, order.Lines?.Count ?? 0);
                Assert.Equal("Apples", order.Lines[0].Product);
                // But the patch still ran — audit fields are set
                Assert.Equal(2, order.LastDeletedLine);
                Assert.Equal(1, order.DeleteCount);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ThreeWayNesting_WithPatches_InsertDeleteOrdering()
        {
            // 3-level nesting with patches at root and department level.
            // Tests that:
            // 1. Root patch computes a field from unmapped columns
            // 2. Embedded patch on departments sets a computed field on the root doc
            // 3. CDC streaming inserts and deletes at all levels work correctly
            // 4. Operations within a single transaction are applied in order
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    country VARCHAR(100) NOT NULL
                );
                CREATE TABLE departments (
                    company_id INT NOT NULL REFERENCES companies(id),
                    dept_id INT NOT NULL,
                    dept_name VARCHAR(200) NOT NULL,
                    budget NUMERIC(12,2) NOT NULL DEFAULT 0,
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
                INSERT INTO companies (id, name, country) VALUES (1, 'Acme Corp', 'US');
                INSERT INTO departments (company_id, dept_id, dept_name, budget) VALUES (1, 10, 'Engineering', 500000);
                INSERT INTO departments (company_id, dept_id, dept_name, budget) VALUES (1, 20, 'Sales', 300000);
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 1, 'Alice');
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 2, 'Bob');
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 3, 'Charlie');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-3way-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Companies",
                        SourceTableSchema = "public",
                        SourceTableName = "companies",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        // Root patch: read the unmapped 'country' column
                        Patch = "this.DisplayName = $row.name + ' (' + $row.country + ')';",
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "dept_id", Name = "DeptId" },
                                    new CdcColumnMapping { Column = "dept_name", Name = "DeptName" }
                                },
                                // Embedded patch: accumulate total budget on the root doc
                                Patch = "this.TotalBudget = (this.TotalBudget || 0) + $row.budget;",
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
                                        Columns = new List<CdcColumnMapping>
                                        {
                                            new CdcColumnMapping { Column = "emp_id", Name = "EmpId" },
                                            new CdcColumnMapping { Column = "emp_name", Name = "EmpName" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for all 3 employees to be nested
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<CompanyWithBudget>("Companies/1");
                if (company?.Departments == null) return 0;
                int total = 0;
                foreach (var dept in company.Departments)
                    total += dept.Employees?.Count ?? 0;
                return total;
            }, 3, timeout: 60_000);

            // Verify initial state: patches applied during initial load
            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<CompanyWithBudget>("Companies/1");
                Assert.Equal("Acme Corp (US)", company.DisplayName);
                // TotalBudget = 500000 + 300000 = 800000 (each dept patch adds its budget)
                Assert.Equal(800000.0, company.TotalBudget, 0);
            }

            // CDC streaming: in a single transaction, add a new employee to Sales
            // and delete one from Engineering
            ExecuteNpgSql(connectionString, """
                BEGIN;
                INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 4, 'Diana');
                DELETE FROM employees WHERE company_id = 1 AND dept_id = 10 AND emp_id = 1;
                COMMIT;
                """);

            // Wait for Engineering to have 1 employee (Alice deleted) and Sales to have 2
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var company = await session.LoadAsync<CompanyWithBudget>("Companies/1");
                var eng = company?.Departments?.FirstOrDefault(d => d.DeptName == "Engineering");
                var sales = company?.Departments?.FirstOrDefault(d => d.DeptName == "Sales");
                return (eng?.Employees?.Count ?? 0, sales?.Employees?.Count ?? 0);
            }, (1, 2), timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<CompanyWithBudget>("Companies/1");

                var eng = company.Departments.First(d => d.DeptName == "Engineering");
                Assert.Single(eng.Employees);
                Assert.Equal("Bob", eng.Employees[0].EmpName);

                var sales = company.Departments.First(d => d.DeptName == "Sales");
                Assert.Equal(2, sales.Employees.Count);
                var salesNames = sales.Employees.Select(e => e.EmpName).OrderBy(n => n).ToList();
                Assert.Equal("Charlie", salesNames[0]);
                Assert.Equal("Diana", salesNames[1]);
            }
        }

        private class CompanyWithBudget
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string DisplayName { get; set; }
            public double TotalBudget { get; set; }
            public List<DepartmentWithEmployees> Departments { get; set; }
        }

        private class DepartmentWithEmployees
        {
            public int DeptId { get; set; }
            public string DeptName { get; set; }
            public List<Employee> Employees { get; set; }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EmbeddedPatch_OldRowData_DeltaComputation()
        {
            // Tests that $old is available in embedded patches for delta computations.
            // When an embedded line item's amount changes, the parent's TotalAmount
            // is adjusted by the delta (new - old), not the absolute new value.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE invoices (id SERIAL PRIMARY KEY, customer VARCHAR(200) NOT NULL);
                CREATE TABLE invoice_lines (
                    invoice_id INT NOT NULL REFERENCES invoices(id),
                    line_num INT NOT NULL,
                    description VARCHAR(200) NOT NULL,
                    amount NUMERIC(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num)
                );
                INSERT INTO invoices (id, customer) VALUES (1, 'Acme');
                INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00);
                INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00);
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-old-row-delta",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Invoices",
                        SourceTableSchema = "public",
                        SourceTableName = "invoices",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer", Name = "Customer" }
                        },
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "description", Name = "Description" },
                                    new CdcColumnMapping { Column = "amount", Name = "Amount" }
                                },
                                // $old is null on insert, populated on update.
                                // On insert: adds the full amount (old is null, so $old?.Amount is undefined → || 0)
                                // On update: adjusts by the delta (new amount - old amount)
                                Patch = "this.TotalAmount = (this.TotalAmount || 0) + $row.amount - ($old?.Amount || 0);"
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load: 2 lines, TotalAmount = 100 + 200 = 300
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 300.0, timeout: 60_000);

            // Update line 1: amount changes from 100 to 150
            // Delta = 150 - 100 = +50, so TotalAmount should go from 300 to 350
            ExecuteNpgSql(connectionString, "UPDATE invoice_lines SET amount = 150.00 WHERE invoice_id = 1 AND line_num = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 350.0, timeout: 60_000);

            // Update line 2: amount changes from 200 to 50
            // Delta = 50 - 200 = -150, so TotalAmount should go from 350 to 200
            ExecuteNpgSql(connectionString, "UPDATE invoice_lines SET amount = 50.00 WHERE invoice_id = 1 AND line_num = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 200.0, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                Assert.Equal(200.0, inv.TotalAmount, 2);
                Assert.Equal(2, inv.Lines.Count);
                // Verify the line amounts were updated correctly too
                var line1 = inv.Lines.First(l => l.LineNum == 1);
                Assert.Equal(150.0, (double)line1.Amount, 2);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Root_PatchOnly_AuditThenDelete()
        {
            // OnDelete.Patch without IgnoreDeletes: patch runs, then document is deleted
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                ALTER TABLE orders REPLICA IDENTITY FULL;
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO orders (id, customer_name) VALUES (2, 'Bob');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-patch-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            Patch = "this.DeleteCount = (this.DeleteCount || 0) + 1;"
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            ExecuteNpgSql(connectionString, "DELETE FROM orders WHERE id = 1;");

            // Document should be deleted despite the patch running
            var deleted = await WaitForDocumentDeletionAsync(store, "Orders/1", timeoutMs: 60_000);
            Assert.True(deleted, "Document Orders/1 should be deleted (Patch runs but IgnoreDeletes is false)");

            // Order 2 should still exist
            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<Order>("Orders/2");
                Assert.NotNull(order2);
                Assert.Equal("Bob", order2.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Root_IgnoreDeletesOnly_SilentIgnore()
        {
            // OnDelete.IgnoreDeletes without Patch: DELETE event is silently discarded
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                ALTER TABLE orders REPLICA IDENTITY FULL;
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-ignore-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            IgnoreDeletes = true
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            var doc = await WaitForDocumentAsync<Order>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            // Delete in PostgreSQL, then insert a new row to force CDC progress
            ExecuteNpgSql(connectionString, "DELETE FROM orders WHERE id = 1;");
            ExecuteNpgSql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob');");

            // Wait for the insert to arrive (proves CDC is processing)
            var doc2 = await WaitForDocumentAsync<Order>(store, "Orders/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            // Order 1 should still exist — delete was silently ignored
            using (var session = store.OpenAsyncSession())
            {
                var order1 = await session.LoadAsync<Order>("Orders/1");
                Assert.NotNull(order1);
                Assert.Equal("Alice", order1.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Root_ConditionalDelete()
        {
            // IgnoreDeletes + Patch with conditional del(): only delete sent orders
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL, status VARCHAR(50) NOT NULL);
                ALTER TABLE orders REPLICA IDENTITY FULL;
                INSERT INTO orders (id, customer_name, status) VALUES (1, 'Alice', 'Sent');
                INSERT INTO orders (id, customer_name, status) VALUES (2, 'Bob', 'Pending');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-conditional",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" },
                            new CdcColumnMapping { Column = "status", Name = "Status" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            IgnoreDeletes = true,
                            Patch = """
                                    if (this.Status === 'Sent') {
                                        del(id(this));
                                    }
                                    // else: keep the document (IgnoreDeletes applies)"
                                    """
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete both orders in PostgreSQL
            ExecuteNpgSql(connectionString, """
                BEGIN;
                DELETE FROM orders WHERE id = 1;
                DELETE FROM orders WHERE id = 2;
                COMMIT;
                """);

            // Wait for the sent order to be deleted
            var deleted = await WaitForDocumentDeletionAsync(store, "Orders/1", timeoutMs: 60_000);
            Assert.True(deleted, "Sent order (Orders/1) should be deleted by conditional del()");

            // Pending order should still exist — IgnoreDeletes kept it
            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<OrderWithStatus>("Orders/2");
                Assert.NotNull(order2);
                Assert.Equal("Pending", order2.Status);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Embedded_IgnoreDeletesOnly()
        {
            // Embedded OnDelete.IgnoreDeletes without Patch: item stays in array
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-ignore-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
                                },
                                OnDelete = new CdcSinkOnDeleteConfig
                                {
                                    IgnoreDeletes = true
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
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 2, timeout: 60_000);

            // Delete a line, then insert a new one to prove CDC is advancing
            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1;");
            ExecuteNpgSql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries');");

            // Wait for the new item to arrive
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 3, timeout: 60_000);

            // All 3 lines should be present — the delete was ignored
            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                Assert.Equal(3, order.Lines.Count);
                var products = order.Lines.Select(l => l.Product).OrderBy(p => p).ToList();
                Assert.Contains("Apples", products);
                Assert.Contains("Bananas", products);
                Assert.Contains("Cherries", products);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Embedded_PatchAndIgnoreDeletes_Archive()
        {
            // Embedded IgnoreDeletes + Patch: patch runs on parent, item stays in array
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name VARCHAR(200) NOT NULL);
                CREATE TABLE order_lines (
                    order_id INT NOT NULL REFERENCES orders(id),
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-archive",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = "public",
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
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
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "line_num", Name = "LineNum" },
                                    new CdcColumnMapping { Column = "product", Name = "Product" }
                                },
                                OnDelete = new CdcSinkOnDeleteConfig
                                {
                                    IgnoreDeletes = true,
                                    Patch = "this.LastArchivedLine = $row.line_num; this.ArchiveCount = (this.ArchiveCount || 0) + 1;"
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
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 2, timeout: 60_000);

            ExecuteNpgSql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2;");

            // Wait for the patch side-effect
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                return order?.ArchiveCount;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                // Item stays — IgnoreDeletes prevented removal
                Assert.Equal(2, order.Lines.Count);
                // Patch ran — audit fields set
                Assert.Equal(2, order.LastArchivedLine);
                Assert.Equal(1, order.ArchiveCount);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task OnDelete_Root_InsertThenDeleteInSameTransaction()
        {
            // INSERT then DELETE with OnDelete.Patch in same transaction
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(200) NOT NULL);
                ALTER TABLE items REPLICA IDENTITY FULL;
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-insert-delete-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            Patch = "this.WasDeleted = true;"
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-insert-delete-patch");

            // In a single transaction: insert then immediately delete
            ExecuteNpgSql(connectionString, """
                BEGIN;
                INSERT INTO items (id, name) VALUES (1, 'Ephemeral');
                DELETE FROM items WHERE id = 1;
                COMMIT;
                """);

            // The document should be created by the INSERT, then deleted.
            // The OnDelete.Patch runs but the delete still proceeds.
            // Insert another item to prove CDC advanced past the transaction.
            ExecuteNpgSql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'Permanent');");
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            // Item 1 should be deleted (Patch ran but IgnoreDeletes is false)
            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>("Items/1");
                Assert.Null(item1);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task Patch_AuditTrail_InsertUpdateDeleteInsertUpdate()
        {
            // Verifies that Patch and OnDelete.Patch record a full audit trail of
            // operations via put(), and that the sequence matches the SQL transaction order:
            // INSERT → UPDATE → DELETE → INSERT → UPDATE
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(200) NOT NULL);
                ALTER TABLE items REPLICA IDENTITY FULL;
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-audit-trail",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        // On upsert: record the operation in an audit document
                        Patch = @"
                            put('AuditLog/' + id(this) + '/', {
                                Op: $old ? 'Update' : 'Insert',
                                Name: $row.name,
                                PreviousName: $old ? $old.Name : null,
                                Timestamp: new Date().toISOString(),
                                '@metadata': { '@collection': 'AuditLog' }
                            });",
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            // On delete: record the deletion, then let it proceed
                            Patch = @"
                                put('AuditLog/' + id(this) + '/', {
                                    Op: 'Delete',
                                    Name: $row.name,
                                    Timestamp: new Date().toISOString(),
                                    '@metadata': { '@collection': 'AuditLog' }
                                });"
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-audit-trail");

            // Execute the full sequence in a single transaction:
            // INSERT → UPDATE → DELETE → INSERT → UPDATE
            ExecuteNpgSql(connectionString, """
                BEGIN;
                INSERT INTO items (id, name) VALUES (1, 'Alpha');
                UPDATE items SET name = 'Beta' WHERE id = 1;
                DELETE FROM items WHERE id = 1;
                INSERT INTO items (id, name) VALUES (1, 'Gamma');
                UPDATE items SET name = 'Delta' WHERE id = 1;
                COMMIT;
                """);

            // Wait for the final state
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var item = await session.LoadAsync<Item>("Items/1");
                return item?.Name;
            }, "Delta", timeout: 60_000);

            // Verify the audit trail records the full sequence
            // Expected: Insert(Alpha), Update(Beta), Delete, Insert(Gamma), Update(Delta)
            using (var session = store.OpenAsyncSession())
            {
                var items = (await session.Advanced.LoadStartingWithAsync<AuditEntry>("AuditLog/Items/1/", pageSize: 10)).ToList();
                var audit1 = items[0];
                Assert.Equal("Insert", audit1.Op);
                Assert.Equal("Alpha", audit1.Name);

                var audit2 = items[1];
                Assert.Equal("Update", audit2.Op);
                Assert.Equal("Beta", audit2.Name);
                Assert.Equal("Alpha", audit2.PreviousName);

                var audit3 = items[2];
                Assert.Equal("Delete", audit3.Op);

                var audit4 = items[3];
                Assert.Equal("Insert", audit4.Op);
                Assert.Equal("Gamma", audit4.Name);

                var audit5 = items[4];
                Assert.Equal("Update", audit5.Op);
                Assert.Equal("Delta", audit5.Name);
                Assert.Equal("Gamma", audit5.PreviousName);
            }
        }

        /// <summary>
        /// Full lifecycle test: start with Employees only, let CDC run, then edit
        /// the task to add Cars. Verifies that:
        /// - Initial load works for the first table
        /// - CDC streaming works for the first table
        /// - Editing the task triggers initial load for the newly added table
        /// - CDC streaming works for both tables after the edit
        /// - The publication is updated to cover both tables
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task EditTask_AddSecondTable_InitialLoadAndCdcWorkForBoth()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            // --- Setup: create both tables upfront ---
            ExecuteNpgSql(connectionString, """
                CREATE TABLE employees (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    department VARCHAR(200)
                );
                CREATE TABLE cars (
                    id SERIAL PRIMARY KEY,
                    make VARCHAR(200) NOT NULL,
                    model VARCHAR(200) NOT NULL
                );
                """);

            // Pre-populate both tables
            ExecuteNpgSql(connectionString, """
                INSERT INTO employees (id, name, department) VALUES (1, 'Alice', 'Engineering');
                INSERT INTO employees (id, name, department) VALUES (2, 'Bob', 'Marketing');
                INSERT INTO cars (id, make, model) VALUES (1, 'Toyota', 'Camry');
                INSERT INTO cars (id, make, model) VALUES (2, 'Honda', 'Civic');
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            // --- Phase 1: Create CDC task with Employees only ---
            var config = new CdcSinkConfiguration
            {
                Name = "test-edit-add-table",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Employees",
                        SourceTableSchema = "public",
                        SourceTableName = "employees",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "department", Name = "Department" }
                        }
                    }
                }
            };

            var addResult = AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-edit-add-table");

            // Verify initial load brought in both employees
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Name;
            }, "Alice", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var bob = await session.LoadAsync<EmployeeRecord>("Employees/2");
                Assert.NotNull(bob);
                Assert.Equal("Bob", bob.Name);
            }

            // Cars should NOT exist in RavenDB yet
            using (var session = store.OpenAsyncSession())
            {
                var car = await session.LoadAsync<Car>("Cars/1");
                Assert.Null(car);
            }

            // --- Phase 2: Verify CDC streaming for employees ---
            ExecuteNpgSql(connectionString, "UPDATE employees SET department = 'Management' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Department;
            }, "Management", timeout: 60_000);

            // --- Phase 3: Edit the task to add Cars table ---
            config.TaskId = addResult.TaskId;
            config.Tables.Add(new CdcSinkTableConfig
            {
                CollectionName = "Cars",
                SourceTableSchema = "public",
                SourceTableName = "cars",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new CdcColumnMapping { Column = "id", Name = "DbId" },
                    new CdcColumnMapping { Column = "make", Name = "Make" },
                    new CdcColumnMapping { Column = "model", Name = "Model" }
                }
            });

            store.Maintenance.Send(new UpdateCdcSinkOperation(addResult.TaskId, config));

            // Wait for the new process to pick up and complete initial load for Cars.
            // The process restarts on config change — need to wait for the new instance.
            await AssertWaitForValueAsync(async () =>
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-edit-add-table");
                if (process == null)
                    return false;

                // Wait for initial load to complete (with a short inner timeout)
                var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(500));
                return completed == process.InitialLoadCompleted && process.InitialLoadCompleted.IsCompletedSuccessfully;
            }, true, timeout: 60_000);

            // Verify initial load brought in both cars
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var car = await session.LoadAsync<Car>("Cars/1");
                return car?.Make;
            }, "Toyota", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var car2 = await session.LoadAsync<Car>("Cars/2");
                Assert.NotNull(car2);
                Assert.Equal("Honda", car2.Make);
                Assert.Equal("Civic", car2.Model);
            }

            // --- Phase 4: Verify CDC streaming works for Cars ---
            ExecuteNpgSql(connectionString, "UPDATE cars SET model = 'Accord' WHERE id = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var car = await session.LoadAsync<Car>("Cars/2");
                return car?.Model;
            }, "Accord", timeout: 60_000);

            // --- Phase 5: Verify CDC streaming STILL works for Employees ---
            ExecuteNpgSql(connectionString, "UPDATE employees SET name = 'Alice Smith' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Name;
            }, "Alice Smith", timeout: 60_000);
        }

        private class ComplexDoc
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public string Metadata { get; set; }    // json/jsonb → string
            public string Settings { get; set; }    // jsonb → string
            public List<string> Tags { get; set; }    // text[] → string array
            public string SearchVector { get; set; } // tsvector → string
            public string IpAddress { get; set; }   // inet → string
            public float[] Embedding { get; set; }  // vector → float[]
        }

        /// <summary>
        /// Verifies that PostgreSQL complex types (json, jsonb, text arrays, tsvector, inet, vector)
        /// are handled correctly in both initial load and CDC streaming paths. These types
        /// don't have direct .NET equivalents and must be converted to strings or arrays for JSON storage.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ComplexTypes_Json_Jsonb_Array_TsVector_Inet()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE complex_docs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    metadata JSON,
                    settings JSONB,
                    tags TEXT[],
                    search_vector TSVECTOR,
                    ip_address INET,
                    embedding vector(5)
                );
                INSERT INTO complex_docs (id, name, metadata, settings, tags, search_vector, ip_address, embedding)
                VALUES (
                    1,
                    'TestDoc',
                    '{"key": "value", "nested": {"a": 1}}',
                    '{"theme": "dark", "lang": "en"}',
                    ARRAY['tag1', 'tag2', 'tag3'],
                    to_tsvector('english', 'quick brown fox'),
                    '192.168.1.100',
                    '[0.1, 0.2, 0.3, 0.4, 0.5]'
                );
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-complex-types",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "ComplexDocs",
                        SourceTableSchema = "public",
                        SourceTableName = "complex_docs",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "metadata", Name = "Metadata" },
                            new CdcColumnMapping { Column = "settings", Name = "Settings" },
                            new CdcColumnMapping { Column = "tags", Name = "Tags" },
                            new CdcColumnMapping { Column = "search_vector", Name = "SearchVector" },
                            new CdcColumnMapping { Column = "ip_address", Name = "IpAddress" },
                            new CdcColumnMapping { Column = "embedding", Name = "Embedding" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-complex-types");

            // Verify initial load handled all complex types
            var initialDoc = await WaitForDocumentAsync<ComplexDoc>(store, "ComplexDocs/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);
            Assert.Equal("TestDoc", initialDoc.Name);

            // JSON and JSONB should arrive as their string representation
            Assert.NotNull(initialDoc.Metadata);
            Assert.Contains("key", initialDoc.Metadata);
            Assert.Contains("value", initialDoc.Metadata);

            Assert.NotNull(initialDoc.Settings);
            Assert.Contains("theme", initialDoc.Settings);
            Assert.Contains("dark", initialDoc.Settings);

            // Array, tsvector, inet should all arrive as string representations
            Assert.NotNull(initialDoc.Tags);
            Assert.NotNull(initialDoc.IpAddress);

            // pgvector: embedding should arrive as an array of numbers
            Assert.NotNull(initialDoc.Embedding);
            Assert.Equal(5, initialDoc.Embedding.Length);

            // Capture initial values to compare after CDC update
            var initialMetadata = initialDoc.Metadata;
            var initialTags = initialDoc.Tags;
            var initialIp = initialDoc.IpAddress;

            // --- CDC streaming: update all complex columns ---
            ExecuteNpgSql(connectionString, """
                UPDATE complex_docs SET
                    metadata = '{"key": "updated", "extra": true}',
                    settings = '{"theme": "light", "lang": "fr"}',
                    tags = ARRAY['alpha', 'beta'],
                    search_vector = to_tsvector('english', 'lazy dog jumps'),
                    ip_address = '10.0.0.1',
                    embedding = '[0.9, 0.8, 0.7, 0.6, 0.5]'
                WHERE id = 1;
                """);

            // Verify the CDC update arrives with the same type handling
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<ComplexDoc>("ComplexDocs/1");
                return doc?.Settings;
            }, """{"lang": "fr", "theme": "light"}""", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var updated = await session.LoadAsync<ComplexDoc>("ComplexDocs/1");
                Assert.NotNull(updated);

                // JSON should have new content
                Assert.Contains("updated", updated.Metadata);

                // JSONB content updated
                Assert.Contains("light", updated.Settings);

                // Inet updated
                Assert.Contains("10.0.0", updated.IpAddress);

                // pgvector embedding updated
                Assert.NotNull(updated.Embedding);
                Assert.Equal(5, updated.Embedding.Length);
            }
        }

        /// <summary>
        /// Verifies that patch scripts can read and manipulate complex PostgreSQL types
        /// via $row: json/jsonb arrive as strings (requiring JSON.parse), text arrays
        /// arrive as JS arrays of strings, and pgvector arrives as a JS array of numbers.
        /// The patch script uses these to compute derived properties on the document.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PatchScript_ComplexTypes_Json_Array_Vector()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    metadata JSON,
                    settings JSONB,
                    tags TEXT[],
                    embedding vector(3)
                );
                INSERT INTO items (id, name, metadata, settings, tags, embedding)
                VALUES (
                    1,
                    'TestItem',
                    '{"priority": 5, "label": "urgent"}',
                    '{"enabled": true, "retries": 3}',
                    ARRAY['alpha', 'beta', 'gamma'],
                    '[0.1, 0.2, 0.3]'
                );
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-patch-complex",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = "public",
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        // $row.metadata and $row.settings are strings — must JSON.parse()
                        // $row.tags is a JS array of strings
                        // $row.embedding is a JS array of numbers
                        Patch = @"
                            var meta = JSON.parse($row.metadata);
                            var settings = JSON.parse($row.settings);

                            this.Priority = meta.priority;
                            this.Label = meta.label;
                            this.Enabled = settings.enabled;
                            this.Retries = settings.retries;
                            this.TagCount = $row.tags.length;
                            this.FirstTag = $row.tags[0];
                            this.Tags = $row.tags;
                            this.EmbeddingSum = $row.embedding.reduce(function(a, b) { return a + b; }, 0);
                            this.Embedding = $row.embedding;
                        "
                    }
                }
            };

            AddCdcSink(store, config);

            // --- Verify initial load: patch runs on each row ---
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<PatchedComplexItem>("Items/1");
                return doc?.Name;
            }, "TestItem", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<PatchedComplexItem>("Items/1");
                Assert.NotNull(doc);

                // JSON fields parsed by the script
                Assert.Equal(5, doc.Priority);
                Assert.Equal("urgent", doc.Label);
                Assert.Equal(true, doc.Enabled);
                Assert.Equal(3, doc.Retries);

                // Text array accessed as JS array
                Assert.Equal(3, doc.TagCount);
                Assert.Equal("alpha", doc.FirstTag);
                Assert.Equal(new[] { "alpha", "beta", "gamma" }, doc.Tags);

                // Vector accessed as JS array of numbers
                Assert.True(Math.Abs(0.6 - doc.EmbeddingSum) < 0.001, $"Expected ~0.6, got {doc.EmbeddingSum}");
                Assert.Equal(3, doc.Embedding.Length);
            }

            // --- Verify CDC streaming: update triggers patch with new values ---
            ExecuteNpgSql(connectionString, """
                UPDATE items SET
                    metadata = '{"priority": 10, "label": "low"}',
                    settings = '{"enabled": false, "retries": 0}',
                    tags = ARRAY['delta'],
                    embedding = '[0.9, 0.8, 0.7]'
                WHERE id = 1;
                """);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<PatchedComplexItem>("Items/1");
                return doc?.Label;
            }, "low", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<PatchedComplexItem>("Items/1");
                Assert.NotNull(doc);

                // Updated JSON values
                Assert.Equal(10, doc.Priority);
                Assert.Equal("low", doc.Label);
                Assert.Equal(false, doc.Enabled);
                Assert.Equal(0, doc.Retries);

                // Updated array
                Assert.Equal(1, doc.TagCount);
                Assert.Equal("delta", doc.FirstTag);
                Assert.Equal(new[] { "delta" }, doc.Tags);

                // Updated vector
                Assert.True(Math.Abs(2.4 - doc.EmbeddingSum) < 0.001, $"Expected ~2.4, got {doc.EmbeddingSum}");
            }
        }

        private class PatchedComplexItem
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            // From JSON.parse($row.metadata)
            public int Priority { get; set; }
            public string Label { get; set; }
            // From JSON.parse($row.settings)
            public bool Enabled { get; set; }
            public int Retries { get; set; }
            // From $row.tags (JS array)
            public int TagCount { get; set; }
            public string FirstTag { get; set; }
            public string[] Tags { get; set; }
            // From $row.embedding (JS array of floats)
            public double EmbeddingSum { get; set; }
            public float[] Embedding { get; set; }
        }

        /// <summary>
        /// Verifies that text/string columns (VARCHAR, TEXT, CLOB-equivalent) can be
        /// stored as RavenDB attachments via Attachment-typed column mappings. The source content
        /// is stored as a UTF-8 encoded attachment, allowing large text blobs to be
        /// kept out of the document body.
        /// Also verifies that BYTEA columns continue to work as binary attachments.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task TextAndBinaryColumns_AsAttachments()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE articles (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    body TEXT,
                    summary VARCHAR(1000),
                    thumbnail BYTEA
                );
                INSERT INTO articles (id, title, body, summary, thumbnail)
                VALUES (
                    1,
                    'Hello World',
                    'This is the full article body with lots of text content that should be stored as an attachment.',
                    'A brief summary of the article.',
                    decode('89504E470D0A1A0A', 'hex')
                );
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-text-attachments",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Articles",
                        SourceTableSchema = "public",
                        SourceTableName = "articles",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "title", Name = "Title" },
                            new CdcColumnMapping { Column = "body", Name = "article-body.txt", Type = CdcColumnType.Attachment },
                            new CdcColumnMapping { Column = "summary", Name = "summary.txt", Type = CdcColumnType.Attachment },
                            new CdcColumnMapping { Column = "thumbnail", Name = "thumb.png", Type = CdcColumnType.Attachment }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var doc = await WaitForDocumentAsync<TextAttachmentDoc>(store, "Articles/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("Hello World", doc.Title);

            // Wait for attachments to be written
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var article = await session.LoadAsync<object>("Articles/1");
                return session.Advanced.Attachments.GetNames(article).Length;
            }, 3, timeout: 60_000);

            // Verify the binary attachment (BYTEA → byte[])
            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "thumb.png"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var bytes = ms.ToArray();
                // PNG header: 0x89504E47
                Assert.Equal(0x89, bytes[0]);
                Assert.Equal(0x50, bytes[1]);
            }

            // Verify the text attachment (TEXT → UTF-8 bytes)
            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "article-body.txt"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
                Assert.Contains("full article body", text);
            }

            // Verify the VARCHAR attachment (string → UTF-8 bytes)
            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "summary.txt"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
                Assert.Equal("A brief summary of the article.", text);
            }

            // --- CDC streaming: update the text content ---
            ExecuteNpgSql(connectionString, """
                UPDATE articles SET
                    body = 'Updated article body after CDC streaming.',
                    summary = 'Updated summary.'
                WHERE id = 1;
                """);

            // Verify the text attachment is updated via CDC
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                using var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "article-body.txt");
                if (attachment == null)
                    return "";
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                return System.Text.Encoding.UTF8.GetString(ms.ToArray());
            }, "Updated article body after CDC streaming.", timeout: 60_000);
        }
        /// <summary>
        /// Verifies that the field representations in RavenDB documents are identical
        /// whether they came from the initial load path (DbDataReader.GetValue) or
        /// from the CDC streaming path (ConvertPostgresValue).
        ///
        /// Creates a Postgres table with date, numeric, uuid, boolean, integer and real
        /// columns, inserts a row (initial load), then updates it (CDC stream), and
        /// asserts that every non-name field has the same serialized value in both cases.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task PostgresTypeConsistency_InitialLoadVsCdcStream_DateAndOtherTypes()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE employees (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    birthday DATE NOT NULL,
                    salary NUMERIC(10,2) NOT NULL,
                    employee_id UUID NOT NULL,
                    active BOOLEAN NOT NULL,
                    age INTEGER NOT NULL,
                    score REAL NOT NULL
                )");

            ExecuteNpgSql(connectionString, @"
                INSERT INTO employees (id, name, birthday, salary, employee_id, active, age, score)
                VALUES (1, 'Alice', '1990-06-15', 75000.50, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', true, 33, 4.5)");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-type-consistency",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "TypeEmployees",
                        SourceTableSchema = "public",
                        SourceTableName = "employees",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "birthday", Name = "Birthday" },
                            new CdcColumnMapping { Column = "salary", Name = "Salary" },
                            new CdcColumnMapping { Column = "employee_id", Name = "EmployeeId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" },
                            new CdcColumnMapping { Column = "age", Name = "Age" },
                            new CdcColumnMapping { Column = "score", Name = "Score" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load to bring the document in
            var count = await WaitForDocumentCountAsync(store, "TypeEmployees", expectedCount: 1, timeoutMs: 60_000);
            Assert.Equal(1, count);

            // Capture the field representations after initial load
            string initialBirthday, initialSalary, initialEmployeeId, initialAge, initialScore;
            bool initialActive;
            using (var session = store.OpenAsyncSession())
            {
                var emp = await session.LoadAsync<EmployeeStringFields>("TypeEmployees/1");
                Assert.NotNull(emp);
                Assert.Equal("Alice", emp.Name);
                initialBirthday = emp.Birthday;
                initialSalary = emp.Salary;
                initialEmployeeId = emp.EmployeeId;
                initialActive = emp.Active;
                initialAge = emp.Age;
                initialScore = emp.Score;
            }

            // Update via CDC stream — only change the name so all other fields stay the same
            ExecuteNpgSql(connectionString, @"
                UPDATE employees SET name = 'Alice Updated' WHERE id = 1");

            // Wait for CDC to propagate the update
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeStringFields>("TypeEmployees/1");
                return emp?.Name;
            }, "Alice Updated", timeout: 60_000);

            // Verify field representations are identical between initial load and CDC update
            using (var session = store.OpenAsyncSession())
            {
                var emp = await session.LoadAsync<EmployeeStringFields>("TypeEmployees/1");
                Assert.NotNull(emp);
                Assert.Equal("Alice Updated", emp.Name);

                Assert.Equal(initialBirthday, emp.Birthday);
                Assert.Equal(initialSalary, emp.Salary);
                Assert.Equal(initialEmployeeId, emp.EmployeeId);
                Assert.Equal(initialActive, emp.Active);
                Assert.Equal(initialAge, emp.Age);
                Assert.Equal(initialScore, emp.Score);
            }
        }

        /// <summary>
        /// Verifies that NUMERIC columns with high precision (more digits than double can represent)
        /// survive both initial load (keyset pagination) and CDC streaming without precision loss.
        /// Before the fix, ConvertStringToType parsed NUMERIC as double (15-17 significant digits),
        /// silently losing precision for values like 1234567890.123456789.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task NumericPrecision_InitialLoadAndCdcStream_NoLoss()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE accounts (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    balance NUMERIC(28,12) NOT NULL
                )");

            // Value that exceeds double precision (15-17 significant digits).
            // double would produce 1234567890.12346 (5 decimal digits), losing 7 digits of precision.
            ExecuteNpgSql(connectionString, @"
                INSERT INTO accounts (id, name, balance)
                VALUES (1, 'Precision Test', 1234567890.123456789012),
                       (2, 'Second Row', 9999999999999999.999999999999)");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-numeric-precision",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Accounts",
                        SourceTableSchema = "public",
                        SourceTableName = "accounts",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "balance", Name = "Balance" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load (exercises ConvertStringToType with NUMERIC → decimal.Parse)
            var count = await WaitForDocumentCountAsync(store, "Accounts", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Verify precision survived initial load
            string initialBalance;
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<AccountDoc>("Accounts/1");
                Assert.NotNull(doc);
                Assert.Equal("Precision Test", doc.Name);
                // decimal preserves trailing zeros and full precision
                Assert.Equal(1234567890.123456789012M, doc.Balance);
                initialBalance = doc.Balance.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            // Update via CDC stream — triggers ConvertPostgresValue (PostgresTypeCategory.Numeric → decimal)
            ExecuteNpgSql(connectionString, @"
                UPDATE accounts SET name = 'Precision Updated' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<AccountDoc>("Accounts/1");
                return doc?.Name;
            }, "Precision Updated", timeout: 60_000);

            // Verify precision is identical after CDC update
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<AccountDoc>("Accounts/1");
                Assert.NotNull(doc);
                var cdcBalance = doc.Balance.ToString(System.Globalization.CultureInfo.InvariantCulture);
                Assert.Equal(initialBalance, cdcBalance);
            }
        }

        private class AccountDoc
        {
            public string Name { get; set; }
            public decimal Balance { get; set; }
        }

        /// <summary>
        /// Verifies that JSON and JSONB columns declared with CdcColumnType.Json are stored as
        /// native JSON objects/arrays in the RavenDB document (not as escaped strings).
        /// Tests both initial load and CDC streaming to confirm both paths handle
        /// JSON columns identically.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task JsonColumns_StoredAsNativeJsonObjects()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.NpgSQL, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, """
                CREATE TABLE configs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    settings JSONB NOT NULL,
                    tags JSON,
                    description TEXT
                );
                INSERT INTO configs (id, name, settings, tags, description)
                VALUES (
                    1,
                    'AppConfig',
                    '{"theme": "dark", "notifications": {"email": true, "sms": false}}',
                    '["production", "v2"]',
                    'Main application configuration'
                );
                """);

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-json-columns",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Configs",
                        SourceTableSchema = "public",
                        SourceTableName = "configs",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "settings", Name = "Settings", Type = CdcColumnType.Json },
                            new CdcColumnMapping { Column = "tags", Name = "Tags", Type = CdcColumnType.Json },
                            new CdcColumnMapping { Column = "description", Name = "Description" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-json-columns");

            // Verify initial load: JSON columns are native objects, not strings
            var doc = await WaitForDocumentAsync<ConfigDoc>(store, "Configs/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("AppConfig", doc.Name);
            Assert.Equal("Main application configuration", doc.Description);

            // settings should be a nested object with theme and notifications
            Assert.NotNull(doc.Settings);
            Assert.Equal("dark", doc.Settings.Theme);
            Assert.NotNull(doc.Settings.Notifications);
            Assert.True(doc.Settings.Notifications.Email);
            Assert.False(doc.Settings.Notifications.Sms);

            // tags should be an array
            Assert.NotNull(doc.Tags);
            Assert.Equal(2, doc.Tags.Count);
            Assert.Equal("production", doc.Tags[0]);
            Assert.Equal("v2", doc.Tags[1]);

            // --- CDC streaming: update the JSON columns ---
            ExecuteNpgSql(connectionString, """
                UPDATE configs SET
                    settings = '{"theme": "light", "notifications": {"email": false, "sms": true}, "newField": 42}',
                    tags = '["staging", "v3", "beta"]'
                WHERE id = 1;
                """);

            // Verify the CDC update preserves native JSON structure
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var updated = await session.LoadAsync<ConfigDoc>("Configs/1");
                return updated?.Settings?.Theme;
            }, "light", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var updated = await session.LoadAsync<ConfigDoc>("Configs/1");
                Assert.NotNull(updated);

                // Updated JSONB: new theme, flipped notifications, new field
                Assert.Equal("light", updated.Settings.Theme);
                Assert.False(updated.Settings.Notifications.Email);
                Assert.True(updated.Settings.Notifications.Sms);

                // Updated JSON array: new tags
                Assert.Equal(3, updated.Tags.Count);
                Assert.Equal("staging", updated.Tags[0]);
                Assert.Equal("v3", updated.Tags[1]);
                Assert.Equal("beta", updated.Tags[2]);

                // Non-JSON column unchanged
                Assert.Equal("Main application configuration", updated.Description);
            }
        }

    }
}
