using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    /// <summary>
    /// MySQL CDC tests run sequentially to avoid binlog position conflicts.
    /// </summary>
    [CollectionDefinition(nameof(CdcSinkMySqlTests), DisableParallelization = true)]
    public class CdcSinkMySqlTests;

    [Collection(nameof(CdcSinkMySqlTests))]
    public class CdcSinkMySqlIntegrationTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkMySqlIntegrationTests(Xunit.ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMySql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, connectionString, sql);
        }

        /// <summary>
        /// Executes multiple SQL statements in a single MySQL transaction.
        /// Each statement is executed separately but within the same BEGIN/COMMIT.
        /// </summary>
        private void ExecuteMySqlInTransaction(string connectionString, params string[] statements)
        {
            using var conn = new MySqlConnector.MySqlConnection(connectionString);
            conn.Open();
            using var tx = conn.BeginTransaction();
            foreach (var sql in statements)
            {
                using var cmd = new MySqlConnector.MySqlCommand(sql, conn, tx);
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string connectionString, string name = "mysql-cdc-test")
        {
            var sqlCs = new SqlConnectionString
            {
                Name = name,
                FactoryName = "MySqlConnector.MySqlConnectorFactory",
                ConnectionString = connectionString
            };

            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        /// <summary>
        /// Subscribes to both process-level and document-level error events on a CDC Sink process,
        /// writing errors to the test output for visibility when debugging test failures.
        /// </summary>
        private async Task SubscribeToCdcErrors(IDocumentStore store, string configName)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
            if (process == null)
                return;

            process.ProcessError += ex =>
                Console.Error.WriteLine($"[CDC-TEST] [{configName}] Process error: {ex.Message}");
        }


        // --- DTO classes (MySQL-specific versions with different field names) ---

        private new class Employee
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public string Department { get; set; }
        }

        private new class OrderLine
        {
            public int LineNum { get; set; }
            public string Product { get; set; }
            public int Quantity { get; set; }
        }

        private new class Order
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Customer { get; set; }
            public decimal Total { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        // --- Tests ---

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task InitialLoad_RootTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    department VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                INSERT INTO employees (name, department) VALUES ('Alice', 'Engineering');
                INSERT INTO employees (name, department) VALUES ('Bob', 'Marketing');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-initial-load",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Employees",
                        SourceTableSchema = schemaName,
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

            AddCdcSink(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var alice = await session.LoadAsync<Employee>("Employees/1");
                return alice?.Name;
            }, "Alice", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var alice = await session.LoadAsync<Employee>("Employees/1");
                Assert.NotNull(alice);
                Assert.Equal("Alice", alice.Name);
                Assert.Equal("Engineering", alice.Department);

                var bob = await session.LoadAsync<Employee>("Employees/2");
                Assert.NotNull(bob);
                Assert.Equal("Bob", bob.Name);
                Assert.Equal("Marketing", bob.Department);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-streaming-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = schemaName,
                        SourceTableName = "products",
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
            await WaitForCdcInitialLoadAsync(store, "test-mysql-streaming-insert");

            // Insert via CDC streaming (after initial load)
            ExecuteMySql(connectionString, "INSERT INTO products (name) VALUES ('Widget');");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Products/1");
                return (string)doc?.Name;
            }, "Widget", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task CdcStreaming_Update()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'active'
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (name) VALUES ('Item1');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-streaming-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = schemaName,
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "status", Name = "Status" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Items/1");
                return (string)doc?.Name;
            }, "Item1", timeout: 60_000);

            // Update via CDC
            ExecuteMySql(connectionString, "UPDATE items SET status = 'archived' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Items/1");
                return (string)doc?.Status;
            }, "archived", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task CdcStreaming_Delete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE notes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    content VARCHAR(500) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO notes (content) VALUES ('Hello World');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-streaming-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Notes",
                        SourceTableSchema = schemaName,
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

            // Wait for initial load
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Notes/1");
                return (string)doc?.Content;
            }, "Hello World", timeout: 60_000);

            // Delete via CDC
            ExecuteMySql(connectionString, "DELETE FROM notes WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Notes/1");
                return doc == null;
            }, true, timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task JsonColumns_StoredAsNativeJsonObjects()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE configs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    settings JSON NOT NULL,
                    tags JSON,
                    description TEXT
                )");

            ExecuteMySql(connectionString, @"
                INSERT INTO configs (id, name, settings, tags, description)
                VALUES (
                    1,
                    'AppConfig',
                    '{""theme"": ""dark"", ""notifications"": {""email"": true, ""sms"": false}}',
                    '[""production"", ""v2""]',
                    'Main application configuration'
                )");

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
                        SourceTableSchema = schemaName,
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

            var doc = await WaitForDocumentAsync<ConfigDoc>(store, "Configs/1", timeoutMs: 60_000);
            Assert.NotNull(doc);
            Assert.Equal("AppConfig", doc.Name);
            Assert.Equal("Main application configuration", doc.Description);

            Assert.NotNull(doc.Settings);
            Assert.Equal("dark", doc.Settings.Theme);
            Assert.NotNull(doc.Settings.Notifications);
            Assert.True(doc.Settings.Notifications.Email);
            Assert.False(doc.Settings.Notifications.Sms);

            Assert.NotNull(doc.Tags);
            Assert.Equal(2, doc.Tags.Count);
            Assert.Equal("production", doc.Tags[0]);
            Assert.Equal("v2", doc.Tags[1]);

            // CDC streaming: update the JSON columns
            ExecuteMySql(connectionString, @"
                UPDATE configs SET
                    settings = '{""theme"": ""light"", ""notifications"": {""email"": false, ""sms"": true}, ""newField"": 42}',
                    tags = '[""staging"", ""v3"", ""beta""]'
                WHERE id = 1");

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

                Assert.Equal("light", updated.Settings.Theme);
                Assert.False(updated.Settings.Notifications.Email);
                Assert.True(updated.Settings.Notifications.Sms);

                Assert.Equal(3, updated.Tags.Count);
                Assert.Equal("staging", updated.Tags[0]);
                Assert.Equal("v3", updated.Tags[1]);
                Assert.Equal("beta", updated.Tags[2]);

                Assert.Equal("Main application configuration", updated.Description);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PatchWithDollarRow()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO people (id, first_name, last_name) VALUES (1, 'John', 'Doe')");

            ExecuteMySql(connectionString, "INSERT INTO people (id, first_name, last_name) VALUES (2, 'Jane', 'Smith')");

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
                        SourceTableSchema = schemaName,
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PatchScript_ModifiesMappedData()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    base_price DECIMAL(10,2) NOT NULL,
                    tax_rate DECIMAL(5,2) NOT NULL DEFAULT 0
                )");

            ExecuteMySql(connectionString, "INSERT INTO products (id, name, base_price, tax_rate) VALUES (1, 'Widget', 100.00, 0.20)");

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
                        SourceTableSchema = schemaName,
                        SourceTableName = "products",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PatchOnDelete_RootTable_ArchivePattern()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob')");

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
                        SourceTableSchema = schemaName,
                        SourceTableName = "orders",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "customer_name", Name = "CustomerName" }
                        },
                        OnDelete = new CdcSinkOnDeleteConfig
                        {
                            IgnoreDeletes = true,
                            Patch = "this.Archived = true; this.ArchivedAt = new Date().toISOString();"
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete order 1 in MySQL
            ExecuteMySql(connectionString, "DELETE FROM orders WHERE id = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Root_ConditionalDelete()
        {
            // IgnoreDeletes + Patch with conditional del(): only delete sent orders
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL,
                    status VARCHAR(50) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name, status) VALUES (1, 'Alice', 'Sent')");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name, status) VALUES (2, 'Bob', 'Pending')");

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
                        SourceTableSchema = schemaName,
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
                            Patch = @"
                                if (this.Status === 'Sent') {
                                    del(id(this));
                                }"
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Delete both orders in MySQL (separate statements)
            ExecuteMySql(connectionString, "DELETE FROM orders WHERE id = 1");

            ExecuteMySql(connectionString, "DELETE FROM orders WHERE id = 2");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PropertyRetention_OnUpdate()
        {
            // Verify that fields set directly in RavenDB are preserved when a CDC update arrives
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    email VARCHAR(200)
                )");

            ExecuteMySql(connectionString, "INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

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
                        SourceTableSchema = schemaName,
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

            // Now update the row in MySQL
            ExecuteMySql(connectionString, "UPDATE customers SET name = 'Alice Updated' WHERE id = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task InitialLoad_WithColumnMapping()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    product_id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO items (product_id, product_name) VALUES (1, 'Alpha')");

            ExecuteMySql(connectionString, "INSERT INTO items (product_id, product_name) VALUES (2, 'Beta')");

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
                        SourceTableSchema = schemaName,
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task LinkedTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE customers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_id INT NOT NULL,
                    total DECIMAL(12,2) NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO customers (id, name) VALUES (42, 'Big Corp')");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_id, total) VALUES (1, 42, 150.00)");
            // Precision-sensitive value: 123456789.01 cannot survive a decimal→double→decimal round-trip
            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_id, total) VALUES (2, 42, 123456789.01)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            var doc2 = await WaitForDocumentAsync<Order>(store, "Orders/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);
            Assert.Equal(123456789.01m, doc2.Total);
        }
        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedPatch_OldRowData_DeltaComputation()
        {
            // Tests that $old is available in embedded patches for delta computations.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE invoices (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE invoice_lines (
                    invoice_id INT NOT NULL,
                    line_num INT NOT NULL,
                    description VARCHAR(200) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num),
                    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO invoices (id, customer) VALUES (1, 'Acme')");

            ExecuteMySql(connectionString, "INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00)");

            ExecuteMySql(connectionString, "INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
                                Patch = "this.TotalAmount = (this.TotalAmount || 0) + $row.amount - ($old?.Amount || 0);"
                            }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await SubscribeToCdcErrors(store, "test-old-row-delta");

            // Wait for initial load: 2 lines, TotalAmount = 100 + 200 = 300
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 300.0, timeout: 60_000);

            // Update line 1: amount changes from 100 to 150
            // Delta = 150 - 100 = +50, so TotalAmount should go from 300 to 350
            ExecuteMySql(connectionString, "UPDATE invoice_lines SET amount = 150.00 WHERE invoice_id = 1 AND line_num = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 350.0, timeout: 60_000);

            // Update line 2: amount changes from 200 to 50
            // Delta = 50 - 200 = -150, so TotalAmount should go from 350 to 200
            ExecuteMySql(connectionString, "UPDATE invoice_lines SET amount = 50.00 WHERE invoice_id = 1 AND line_num = 2");

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
                var line1 = inv.Lines.First(l => l.LineNum == 1);
                Assert.Equal(150.0, (double)line1.Amount, 2);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            var doc = await WaitForDocumentAsync<Order>(store, "Orders/1", timeoutMs: 60_000);
            Assert.NotNull(doc);

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5)");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_Delete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 3, 'Cherries', 7)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
            }, 3, timeout: 60_000);

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_Delete_NonCompositePK()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (3, 1, 'Cherries', 7)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 3, timeout: 60_000);

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE id = 2");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_Update()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5)");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "UPDATE order_lines SET quantity = 99, product = 'Bananas (Updated)' WHERE order_id = 1 AND line_num = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return null;
                var line = order.Lines.FirstOrDefault(l => l.LineNum == 2);
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

        /// <summary>
        /// UPDATE that changes only the FK (join column) on an embedded table whose own PK is
        /// unchanged. MySQL binlog UpdateRowsEvent carries both BeforeUpdate and AfterUpdate
        /// row images (binlog_row_image=FULL is the server default), so
        /// CdcSinkProcess.CreateEmbeddedUpdateEvents must produce a Delete against the old
        /// parent and an Upsert against the new parent in the same transaction.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_Reparent_OnJoinColumnChange()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    id INT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");
            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob')");
            ExecuteMySql(connectionString, "INSERT INTO order_lines (id, order_id, product, quantity) VALUES (10, 1, 'Apples', 7)");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-reparent",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            // Initial load materializes the row in Orders/1.Lines.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order1 = await session.LoadAsync<Order>("Orders/1");
                return order1?.Lines?.Count ?? 0;
            }, 1, timeout: 60_000);

            // Reparenting UPDATE: changes only the join column.
            ExecuteMySql(connectionString, "UPDATE order_lines SET order_id = 2 WHERE id = 10");

            // Orders/1.Lines must lose the row.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order1 = await session.LoadAsync<Order>("Orders/1");
                return order1?.Lines?.Count ?? 0;
            }, 0, timeout: 60_000);

            // Orders/2.Lines must gain the row.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order2 = await session.LoadAsync<Order>("Orders/2");
                return order2?.Lines?.Count ?? 0;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<Order>("Orders/2");
                Assert.Single(order2.Lines);
                Assert.Equal("Apples", order2.Lines[0].Product);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EmbeddedArray_AddAndRemoveInSameTransaction()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1",
                "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries')");

            // Should end up with 2 lines: Bananas (2) and Cherries (3) — Apples (1) deleted
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return false;
                var products = order.Lines.Select(l => l.Product).OrderBy(p => p).ToList();
                return products.Contains("Cherries") && !products.Contains("Apples");
            }, true, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                var lines = order.Lines;
                Assert.Equal(2, lines.Count);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PatchScript_CombinedRootAndEmbedded()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE invoices (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer VARCHAR(200) NOT NULL,
                    discount_pct DECIMAL(5,2) DEFAULT 0
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE invoice_lines (
                    invoice_id INT NOT NULL,
                    line_num INT NOT NULL,
                    description VARCHAR(200) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num),
                    FOREIGN KEY (invoice_id) REFERENCES invoices(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO invoices (id, customer, discount_pct) VALUES (1, 'Big Corp', 10.00)");

            ExecuteMySql(connectionString, "INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00)");

            ExecuteMySql(connectionString, "INSERT INTO invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00)");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            using (var session = store.OpenAsyncSession())
            {
                var inv = await session.LoadAsync<Invoice>("Invoices/1");
                Assert.Equal(200.00, inv.LineAmount, 2);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task PatchOnDelete_EmbeddedTable()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                return order?.DeleteCount;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                Assert.Equal(1, order.Lines?.Count ?? 0);
                Assert.Equal("Apples", order.Lines[0].Product);
                Assert.Equal(2, order.LastDeletedLine);
                Assert.Equal(1, order.DeleteCount);
            }
        }
        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Root_IgnoreDeletesOnly_SilentIgnore()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

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
                        SourceTableSchema = schemaName,
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
            await SubscribeToCdcErrors(store, "test-root-ignore-only");
            await WaitForCdcInitialLoadAsync(store, "test-root-ignore-only");
            await AssertWaitForValueAsync(async () => { using var s = store.OpenAsyncSession(); var o = await s.LoadAsync<Order>("Orders/1"); return o?.CustomerName; }, "Alice", timeout: 60_000);



            ExecuteMySql(connectionString, "DELETE FROM orders WHERE id = 1");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob')");

            var doc2 = await WaitForDocumentAsync<Order>(store, "Orders/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            using (var session = store.OpenAsyncSession())
            {
                var order1 = await session.LoadAsync<Order>("Orders/1");
                Assert.NotNull(order1);
                Assert.Equal("Alice", order1.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Root_PatchOnly_AuditThenDelete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob')");

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
                        SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "DELETE FROM orders WHERE id = 1");

            var deleted = await WaitForDocumentDeletionAsync(store, "Orders/1", timeoutMs: 60_000);
            Assert.True(deleted, "Document Orders/1 should be deleted (Patch runs but IgnoreDeletes is false)");

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<Order>("Orders/2");
                Assert.NotNull(order2);
                Assert.Equal("Bob", order2.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Root_InsertThenDeleteInSameTransaction()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

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
                        SourceTableSchema = schemaName,
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
            await SubscribeToCdcErrors(store, "test-insert-delete-patch");
            await WaitForCdcInitialLoadAsync(store, "test-insert-delete-patch");

            // In a single transaction: insert then immediately delete
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO items (id, name) VALUES (1, 'Ephemeral')",
                "DELETE FROM items WHERE id = 1");

            // Insert another item to prove CDC advanced past the transaction
            ExecuteMySql(connectionString, "INSERT INTO items (id, name) VALUES (2, 'Permanent')");
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>("Items/1");
                Assert.Null(item1);
            }
        }
        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Embedded_IgnoreDeletesOnly()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
            await SubscribeToCdcErrors(store, "test-emb-ignore-only");
            await WaitForCdcInitialLoadAsync(store, "test-emb-ignore-only");
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 2, timeout: 60_000);

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.Lines?.Count ?? 0;
            }, 3, timeout: 60_000);

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task OnDelete_Embedded_PatchAndIgnoreDeletes_Archive()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                return order?.ArchiveCount;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<OrderWithDeleteTracking>("Orders/1");
                Assert.Equal(2, order.Lines.Count);
                Assert.Equal(2, order.LastArchivedLine);
                Assert.Equal(1, order.ArchiveCount);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Patch_AuditTrail_InsertUpdateDeleteInsertUpdate()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

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
                        SourceTableSchema = schemaName,
                        SourceTableName = "items",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
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
            // INSERT -> UPDATE -> DELETE -> INSERT -> UPDATE
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO items (id, name) VALUES (1, 'Alpha')",
                "UPDATE items SET name = 'Beta' WHERE id = 1",
                "DELETE FROM items WHERE id = 1",
                "INSERT INTO items (id, name) VALUES (1, 'Gamma')",
                "UPDATE items SET name = 'Delta' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var item = await session.LoadAsync<Item>("Items/1");
                return item?.Name;
            }, "Delta", timeout: 60_000);

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task BinaryColumn_RootAttachment()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE files (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    content BLOB
                )");

            ExecuteMySql(connectionString, "INSERT INTO files (id, name, content) VALUES (1, 'readme.txt', UNHEX('48656C6C6F20576F726C64'))");

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
                        SourceTableSchema = schemaName,
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task BinaryColumn_EmbeddedAttachment()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE albums (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE photos (
                    album_id INT NOT NULL,
                    photo_num INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    thumbnail BLOB,
                    PRIMARY KEY (album_id, photo_num),
                    FOREIGN KEY (album_id) REFERENCES albums(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO albums (id, name) VALUES (1, 'Vacation')");

            ExecuteMySql(connectionString, "INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', UNHEX('89504E47'))");

            ExecuteMySql(connectionString, "INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', UNHEX('FFD8FFE0'))");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            using (var session = store.OpenAsyncSession())
            {
                var album = await session.LoadAsync<object>("Albums/1");
                var attachments = session.Advanced.Attachments.GetNames(album);
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
                Assert.Equal(0x89, ms.ToArray()[0]);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task DeleteAttachment_OnEmbeddedDelete()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE albums (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE photos (
                    album_id INT NOT NULL,
                    photo_num INT NOT NULL,
                    title VARCHAR(200) NOT NULL,
                    thumbnail BLOB,
                    PRIMARY KEY (album_id, photo_num),
                    FOREIGN KEY (album_id) REFERENCES albums(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO albums (id, name) VALUES (1, 'Vacation')");

            ExecuteMySql(connectionString, "INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', UNHEX('89504E47'))");

            ExecuteMySql(connectionString, "INSERT INTO photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', UNHEX('FFD8FFE0'))");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
                var album = await session.LoadAsync<object>("Albums/1");
                if (album == null) return 0;
                return session.Advanced.Attachments.GetNames(album).Length;
            }, 2, timeout: 60_000);

            ExecuteMySql(connectionString, "DELETE FROM photos WHERE album_id = 1 AND photo_num = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TextAndBinaryColumns_AsAttachments()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE articles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    body TEXT,
                    summary VARCHAR(1000),
                    thumbnail BLOB
                )");

            ExecuteMySql(connectionString,
                "INSERT INTO articles (id, title, body, summary, thumbnail) VALUES (1, 'Hello World', 'This is the full article body with lots of text content that should be stored as an attachment.', 'A brief summary of the article.', UNHEX('89504E470D0A1A0A'))");

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
                        SourceTableSchema = schemaName,
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

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var article = await session.LoadAsync<object>("Articles/1");
                return session.Advanced.Attachments.GetNames(article).Length;
            }, 3, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "thumb.png"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var bytes = ms.ToArray();
                Assert.Equal(0x89, bytes[0]);
                Assert.Equal(0x50, bytes[1]);
            }

            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "article-body.txt"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
                Assert.Contains("full article body", text);
            }

            using (var session = store.OpenAsyncSession())
            using (var attachment = await session.Advanced.Attachments.GetAsync("Articles/1", "summary.txt"))
            {
                Assert.NotNull(attachment);
                using var ms = new System.IO.MemoryStream();
                await attachment.Stream.CopyToAsync(ms);
                var text = System.Text.Encoding.UTF8.GetString(ms.ToArray());
                Assert.Equal("A brief summary of the article.", text);
            }

            ExecuteMySql(connectionString, "UPDATE articles SET body = 'Updated article body after CDC streaming.', summary = 'Updated summary.' WHERE id = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task MetadataExpires_ViaPatch()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE events (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    expires_at DATETIME
                )");

            ExecuteMySql(connectionString, "INSERT INTO events (id, title, expires_at) VALUES (1, 'Flash Sale', '2099-12-31 23:59:59')");

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
                        SourceTableSchema = schemaName,
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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ThreeWayNesting()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE departments (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    dept_name VARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id),
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE employees (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    emp_id INT NOT NULL,
                    emp_name VARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id, emp_id),
                    FOREIGN KEY (company_id, dept_id) REFERENCES departments(company_id, dept_id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO companies (id, name) VALUES (1, 'Acme Corp')");

            ExecuteMySql(connectionString, "INSERT INTO departments (company_id, dept_id, dept_name) VALUES (1, 10, 'Engineering')");

            ExecuteMySql(connectionString, "INSERT INTO departments (company_id, dept_id, dept_name) VALUES (1, 20, 'Sales')");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 2, 'Bob')");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 3, 'Charlie')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
                                        SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "DELETE FROM employees WHERE company_id = 1 AND dept_id = 10 AND emp_id = 1");

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

                var sales = company.Departments.First(d => d.DeptName == "Sales");
                Assert.Single(sales.Employees);
                Assert.Equal("Charlie", sales.Employees[0].EmpName);
            }

            ExecuteMySql(connectionString, "DELETE FROM employees WHERE company_id = 1 AND dept_id = 10");

            ExecuteMySql(connectionString, "DELETE FROM departments WHERE company_id = 1 AND dept_id = 10");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ChildBeforeParent()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                )");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 1, timeout: 60_000);

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                return order?.CustomerName;
            }, "Alice", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("Orders/1");
                Assert.Equal("Alice", order.CustomerName);
                var lines = order.Lines;
                Assert.Single(lines);
                Assert.Equal("Apples", lines[0].Product);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task MultipleUpdates_SameRow_SameTransaction()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE counters (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    value INT NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO counters (id, name, value) VALUES (1, 'hits', 0)");

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
                        SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "UPDATE counters SET value = 1 WHERE id = 1",
                "UPDATE counters SET value = 2 WHERE id = 1",
                "UPDATE counters SET value = 3 WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var counter = await session.LoadAsync<Counter>("Counters/1");
                return (int?)counter?.Value;
            }, 3, timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Transaction_InsertUpdateDeleteInsert_SameRow()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL
                )");

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
                        SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO items (id, name) VALUES (1, 'First')",
                "UPDATE items SET name = 'Second' WHERE id = 1",
                "DELETE FROM items WHERE id = 1",
                "INSERT INTO items (id, name) VALUES (1, 'Final')");

            // The final state should be the last insert
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var item = await session.LoadAsync<Item>("Items/1");
                return item?.Name;
            }, "Final", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Transaction_MultipleDistinctRootDocuments()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    price DECIMAL(10,2) NOT NULL
                )");

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
                        SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)",
                "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)",
                "INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99)");

            var count = await WaitForDocumentCountAsync(store, "Products", expectedCount: 3, timeoutMs: 60_000);
            Assert.Equal(3, count);

            using (var session = store.OpenAsyncSession())
            {
                var p1 = await session.LoadAsync<Product>("Products/1");
                Assert.Equal("Widget", p1.Name);
                var p3 = await session.LoadAsync<Product>("Products/3");
                Assert.Equal("Doohickey", p3.Name);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Transaction_MultipleRootAndEmbedded()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')",
                "INSERT INTO orders (id, customer_name) VALUES (2, 'Bob')",
                "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')",
                "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas')",
                "INSERT INTO order_lines (order_id, line_num, product) VALUES (2, 1, 'Cherries')");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task UpdateParentAndEmbeddedTogether()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    customer_name VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product VARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_name) VALUES (1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples')");

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
                        SourceTableSchema = schemaName,
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
                                SourceTableSchema = schemaName,
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
            ExecuteMySqlInTransaction(connectionString,
                "UPDATE orders SET customer_name = 'Alice Updated' WHERE id = 1",
                "UPDATE order_lines SET product = 'Oranges' WHERE order_id = 1 AND line_num = 1",
                "INSERT INTO order_lines (order_id, line_num, product) VALUES (1, 2, 'Grapes')");

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

            ExecuteMySql(connectionString, "DELETE FROM order_lines WHERE order_id = 1 AND line_num = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task EditTask_AddSecondTable_InitialLoadAndCdcWorkForBoth()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    department VARCHAR(200)
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE cars (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    make VARCHAR(200) NOT NULL,
                    model VARCHAR(200) NOT NULL
                )");

            ExecuteMySql(connectionString, "INSERT INTO employees (id, name, department) VALUES (1, 'Alice', 'Engineering')");

            ExecuteMySql(connectionString, "INSERT INTO employees (id, name, department) VALUES (2, 'Bob', 'Marketing')");

            ExecuteMySql(connectionString, "INSERT INTO cars (id, make, model) VALUES (1, 'Toyota', 'Camry')");

            ExecuteMySql(connectionString, "INSERT INTO cars (id, make, model) VALUES (2, 'Honda', 'Civic')");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-edit-add-table",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Employees",
                        SourceTableSchema = schemaName,
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
            await SubscribeToCdcErrors(store, "test-edit-add-table");
            await WaitForCdcInitialLoadAsync(store, "test-edit-add-table");

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

            using (var session = store.OpenAsyncSession())
            {
                var car = await session.LoadAsync<Car>("Cars/1");
                Assert.Null(car);
            }

            ExecuteMySql(connectionString, "UPDATE employees SET department = 'Management' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Department;
            }, "Management", timeout: 60_000);

            config.TaskId = addResult.TaskId;
            config.Tables.Add(new CdcSinkTableConfig
            {
                CollectionName = "Cars",
                SourceTableSchema = schemaName,
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

            await AssertWaitForValueAsync(async () =>
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == "test-edit-add-table");
                if (process == null)
                    return false;

                var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(500));
                return completed == process.InitialLoadCompleted && process.InitialLoadCompleted.IsCompletedSuccessfully;
            }, true, timeout: 60_000);

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

            ExecuteMySql(connectionString, "UPDATE cars SET model = 'Accord' WHERE id = 2");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var car = await session.LoadAsync<Car>("Cars/2");
                return car?.Model;
            }, "Accord", timeout: 60_000);

            ExecuteMySql(connectionString, "UPDATE employees SET name = 'Alice Smith' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Name;
            }, "Alice Smith", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task ThreeWayNesting_WithPatches_InsertDeleteOrdering()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    country VARCHAR(100) NOT NULL
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE departments (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    dept_name VARCHAR(200) NOT NULL,
                    budget DECIMAL(12,2) NOT NULL DEFAULT 0,
                    PRIMARY KEY (company_id, dept_id),
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                )");

            ExecuteMySql(connectionString, @"
                CREATE TABLE employees (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    emp_id INT NOT NULL,
                    emp_name VARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id, emp_id),
                    FOREIGN KEY (company_id, dept_id) REFERENCES departments(company_id, dept_id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO companies (id, name, country) VALUES (1, 'Acme Corp', 'US')");

            ExecuteMySql(connectionString, "INSERT INTO departments (company_id, dept_id, dept_name, budget) VALUES (1, 10, 'Engineering', 500000)");

            ExecuteMySql(connectionString, "INSERT INTO departments (company_id, dept_id, dept_name, budget) VALUES (1, 20, 'Sales', 300000)");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 1, 'Alice')");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 2, 'Bob')");

            ExecuteMySql(connectionString, "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 3, 'Charlie')");

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
                        SourceTableSchema = schemaName,
                        SourceTableName = "companies",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        },
                        Patch = "this.DisplayName = $row.name + ' (' + $row.country + ')';",
                        EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                        {
                            new CdcSinkEmbeddedTableConfig
                            {
                                SourceTableSchema = schemaName,
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
                                Patch = "this.TotalBudget = (this.TotalBudget || 0) + $row.budget;",
                                EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
                                {
                                    new CdcSinkEmbeddedTableConfig
                                    {
                                        SourceTableSchema = schemaName,
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

            using (var session = store.OpenAsyncSession())
            {
                var company = await session.LoadAsync<CompanyWithBudget>("Companies/1");
                Assert.Equal("Acme Corp (US)", company.DisplayName);
                Assert.Equal(800000.0, company.TotalBudget, 0);
            }

            // CDC streaming: in a single transaction, add a new employee to Sales
            // and delete one from Engineering
            ExecuteMySqlInTransaction(connectionString,
                "INSERT INTO employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 4, 'Diana')",
                "DELETE FROM employees WHERE company_id = 1 AND dept_id = 10 AND emp_id = 1");

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

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TinyInt1_ProducesBooleanJsonType_FromBothInitialLoadAndStreaming()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE flagged (
                    id     INT PRIMARY KEY,
                    active TINYINT(1) NOT NULL DEFAULT 0
                )");

            ExecuteMySql(connectionString, "INSERT INTO flagged (id, active) VALUES (1, 1), (2, 0);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-tinyint1-boolean",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Flagged",
                        SourceTableSchema = schemaName,
                        SourceTableName = "flagged",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-tinyint1-boolean");

            // Baseline (must hold today): initial-load row's Active is a CLR bool because
            // MySqlConnector honours TreatTinyAsBoolean=true and returns bool for tinyint(1).
            using (var commands = store.Commands())
            {
                var doc1 = (await commands.GetAsync("Flagged/1")).BlittableJson;
                Assert.NotNull(doc1);
                Assert.True(doc1.TryGet("Active", out object active1));
                Assert.IsType<bool>(active1);
                Assert.Equal(true, active1);
            }

            // Now exercise the streaming path.
            ExecuteMySql(connectionString, "INSERT INTO flagged (id, active) VALUES (3, 1);");

            // Wait until the streamed doc lands.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("Flagged/3");
                return doc != null;
            }, true, timeout: 60_000);

            // The bug: today the streamed row's Active is a JSON integer (LazyNumberValue),
            // not a bool, because MySqlCdc returns sbyte for tinyint(1) and ConvertMySqlValue
            // widens it to long instead of converting to bool.
            using (var commands = store.Commands())
            {
                var doc3 = (await commands.GetAsync("Flagged/3")).BlittableJson;
                Assert.NotNull(doc3);
                Assert.True(doc3.TryGet("Active", out object active3));
                Assert.IsType<bool>(active3);
                Assert.Equal(true, active3);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Bit1_StaysBoolean_FromBothPaths()
        {
            // Bug: MySQL BIT(1) is stored as a 1-bit value but MySqlConnector returns it
            // as CLR ulong on the initial-load path (no analogous TreatBitAsBoolean flag),
            // so it stringifies via NormalizeForJson's _ => value.ToString() fallback.
            // Streaming behavior depends on what MySqlCdc decodes BIT(1) as - this test
            // captures both observations in one run so we can plan both fix sites at once.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE flagged_bit (
                    id     INT PRIMARY KEY,
                    active BIT(1) NOT NULL DEFAULT b'0'
                )");
            ExecuteMySql(connectionString, "INSERT INTO flagged_bit (id, active) VALUES (1, b'1'), (2, b'0');");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-bit1-boolean",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "FlaggedBit",
                        SourceTableSchema = schemaName,
                        SourceTableName = "flagged_bit",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-bit1-boolean");

            // Stream a third row before any assertion, so we observe both paths even if
            // initial-load fails. (xUnit's default Assert is fail-fast.)
            ExecuteMySql(connectionString, "INSERT INTO flagged_bit (id, active) VALUES (3, b'1');");
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("FlaggedBit/3");
                return doc != null;
            }, true, timeout: 60_000);

            using var commands = store.Commands();

            var doc1 = (await commands.GetAsync("FlaggedBit/1")).BlittableJson;
            Assert.True(doc1.TryGet("Active", out object active1));
            var initialLoadTypeName = active1?.GetType().FullName ?? "null";
            var initialLoadValueRepr = active1?.ToString() ?? "null";

            var doc3 = (await commands.GetAsync("FlaggedBit/3")).BlittableJson;
            Assert.True(doc3.TryGet("Active", out object active3));
            var streamingTypeName = active3?.GetType().FullName ?? "null";
            var streamingValueRepr = active3?.ToString() ?? "null";

            // Combined gate so the failure message reports BOTH observations regardless of
            // which path is broken. Without this, fail-fast would mask the streaming type
            // until initial-load is fixed.
            Assert.True(
                active1 is bool && active3 is bool,
                $"BIT(1) must land as JSON bool on both paths. " +
                $"Initial-load: type={initialLoadTypeName} value={initialLoadValueRepr}. " +
                $"Streaming: type={streamingTypeName} value={streamingValueRepr}.");

            Assert.Equal(true, active1);
            Assert.Equal(true, active3);

            // Also verify the b'0' initial-load row.
            var doc2 = (await commands.GetAsync("FlaggedBit/2")).BlittableJson;
            Assert.True(doc2.TryGet("Active", out object active2));
            Assert.IsType<bool>(active2);
            Assert.Equal(false, active2);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TinyInt1_FalseValue_RoundTripsFromStreaming()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE flags_zero (
                    id     INT PRIMARY KEY,
                    active TINYINT(1) NOT NULL DEFAULT 0
                )");
            ExecuteMySql(connectionString, "INSERT INTO flags_zero (id, active) VALUES (1, 0);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-tinyint1-false",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "FlagsZero",
                        SourceTableSchema = schemaName,
                        SourceTableName = "flags_zero",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-tinyint1-false");

            // Streaming insert with value 0 must produce JSON false, not 0.
            ExecuteMySql(connectionString, "INSERT INTO flags_zero (id, active) VALUES (2, 0);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("FlagsZero/2");
                return doc != null;
            }, true, timeout: 60_000);

            using var commands = store.Commands();
            var doc2 = (await commands.GetAsync("FlagsZero/2")).BlittableJson;
            Assert.True(doc2.TryGet("Active", out object active));
            Assert.IsType<bool>(active);
            Assert.Equal(false, active);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TinyInt1_NullStaysNull_FromStreaming()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE flags_nullable (
                    id     INT PRIMARY KEY,
                    active TINYINT(1) NULL
                )");
            ExecuteMySql(connectionString, "INSERT INTO flags_nullable (id, active) VALUES (1, NULL);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-tinyint1-null",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "FlagsNullable",
                        SourceTableSchema = schemaName,
                        SourceTableName = "flags_nullable",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-tinyint1-null");

            ExecuteMySql(connectionString, "INSERT INTO flags_nullable (id, active) VALUES (2, NULL);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("FlagsNullable/2");
                return doc != null;
            }, true, timeout: 60_000);

            using var commands = store.Commands();

            // Initial-load NULL row. The property must be present-with-null on the document
            // (not silently dropped) — anything else means the value-conversion pipeline lost
            // the explicit-null distinction and a future read returning false/0/missing would
            // be indistinguishable from this row, which breaks downstream patches.
            var doc1 = (await commands.GetAsync("FlagsNullable/1")).BlittableJson;
            Assert.True(doc1.TryGet("Active", out object active1), "Active must be present on the initial-load NULL row");
            Assert.Null(active1);

            // Streamed NULL row.
            var doc2 = (await commands.GetAsync("FlagsNullable/2")).BlittableJson;
            Assert.True(doc2.TryGet("Active", out object active2), "Active must be present on the streamed NULL row");
            Assert.Null(active2);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TinyInt1_UpdateFromOneToZero_FlipsBool_FromStreaming()
        {
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE flags_update (
                    id     INT PRIMARY KEY,
                    active TINYINT(1) NOT NULL DEFAULT 0
                )");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-tinyint1-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "FlagsUpdate",
                        SourceTableSchema = schemaName,
                        SourceTableName = "flags_update",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "active", Name = "Active" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-tinyint1-update");

            // Insert via streaming with active=1, then flip to 0 — UpdateRowsEvent has its own
            // decode path separate from WriteRowsEvent and must also coerce sbyte -> bool.
            ExecuteMySql(connectionString, "INSERT INTO flags_update (id, active) VALUES (1, 1);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("FlagsUpdate/1");
                return (bool?)doc?.Active;
            }, true, timeout: 60_000);

            ExecuteMySql(connectionString, "UPDATE flags_update SET active = 0 WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("FlagsUpdate/1");
                return (bool?)doc?.Active;
            }, false, timeout: 60_000);

            using var commands = store.Commands();
            var doc1 = (await commands.GetAsync("FlagsUpdate/1")).BlittableJson;
            Assert.True(doc1.TryGet("Active", out object active));
            Assert.IsType<bool>(active);
            Assert.Equal(false, active);
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task TinyInt4_StaysInteger_FromBothPaths()
        {
            // Regression guard: tinyint(N) where N != 1 is NOT a boolean. Its values must remain
            // numeric on both paths. If the Boolean category over-matches "any tinyint", this
            // test catches it.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE small_numbers (
                    id  INT PRIMARY KEY,
                    val TINYINT(4) NOT NULL
                )");
            ExecuteMySql(connectionString, "INSERT INTO small_numbers (id, val) VALUES (1, 5), (2, 0);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-tinyint4-integer",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "SmallNumbers",
                        SourceTableSchema = schemaName,
                        SourceTableName = "small_numbers",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "val", Name = "Val" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-tinyint4-integer");

            ExecuteMySql(connectionString, "INSERT INTO small_numbers (id, val) VALUES (3, 7);");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("SmallNumbers/3");
                return doc != null;
            }, true, timeout: 60_000);

            using var commands = store.Commands();
            foreach (var (id, expected) in new[] { ("SmallNumbers/1", 5L), ("SmallNumbers/2", 0L), ("SmallNumbers/3", 7L) })
            {
                var doc = (await commands.GetAsync(id)).BlittableJson;
                Assert.NotNull(doc);
                Assert.True(doc.TryGet("Val", out object val), $"missing Val on {id}");
                Assert.IsType<long>(val);
                Assert.Equal(expected, val);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true)]
        public async Task Smallint_StaysIntegerJsonType_FromBothPaths()
        {
            // Cross-provider Bug #3 regression smoke. MySqlConnector returns Int16 for SMALLINT,
            // but ConvertMySqlValue widens to long on both paths (initial-load via
            // ConvertInitialLoadValue, streaming via the _ => ConvertMySqlValue fallback).
            // Lock the contract.
            using var store = GetDocumentStore();
            using var _ = WithSqlDatabase(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, out var connectionString, out var schemaName, dataSet: null, includeData: false);

            ExecuteMySql(connectionString, @"
                CREATE TABLE smallnums_mysql (
                    id    INT PRIMARY KEY,
                    small SMALLINT NOT NULL
                )");
            ExecuteMySql(connectionString, "INSERT INTO smallnums_mysql (id, small) VALUES (1, 23), (2, -32768);");

            var sqlCs = SetupSqlConnectionString(store, connectionString);
            var config = new CdcSinkConfiguration
            {
                Name = "test-mysql-smallint-cross",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "SmallNumsMySql",
                        SourceTableSchema = schemaName,
                        SourceTableName = "smallnums_mysql",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "small", Name = "Small" }
                        }
                    }
                }
            };
            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-mysql-smallint-cross");

            ExecuteMySql(connectionString, "INSERT INTO smallnums_mysql (id, small) VALUES (3, 100);");
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<dynamic>("SmallNumsMySql/3");
                return doc != null;
            }, true, timeout: 60_000);

            using var commands = store.Commands();
            foreach (var (id, expected) in new[] { ("SmallNumsMySql/1", 23L), ("SmallNumsMySql/2", (long)short.MinValue), ("SmallNumsMySql/3", 100L) })
            {
                var doc = (await commands.GetAsync(id)).BlittableJson;
                Assert.True(doc.TryGet("Small", out object small), $"missing Small on {id}");
                Assert.IsType<long>(small);
                Assert.Equal(expected, small);
            }
        }

        // --- MySQL-specific DTO classes (different nested type structure) ---

        private class NestedEmployee
        {
            public int EmpId { get; set; }
            public string EmpName { get; set; }
        }

        private new class Department
        {
            public int DeptId { get; set; }
            public string DeptName { get; set; }
            public List<NestedEmployee> Employees { get; set; }
        }

        private new class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Department> Departments { get; set; }
        }

        private class CompanyWithBudget
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string DisplayName { get; set; }
            public double TotalBudget { get; set; }
            public List<Department> Departments { get; set; }
        }

        private new class OrderWithDeleteTracking
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public List<OrderLine> Lines { get; set; }
            public int LastDeletedLine { get; set; }
            public int DeleteCount { get; set; }
            public int LastArchivedLine { get; set; }
            public int ArchiveCount { get; set; }
        }
    }
}
