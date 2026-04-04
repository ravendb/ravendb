using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
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
    public class CdcSinkMySqlIntegrationTests : SqlAwareTestBase
    {
        public CdcSinkMySqlIntegrationTests(Xunit.ITestOutputHelper output) : base(output)
        {
        }

        private void ExecuteMySql(string connectionString, string sql)
        {
            ExecuteSqlQuery(Raven.Server.SqlMigration.MigrationProvider.MySQL_MySqlConnector, connectionString, sql);
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

        private AddCdcSinkOperationResult AddCdcSink(IDocumentStore store, CdcSinkConfiguration config)
        {
            return store.Maintenance.Send(new AddCdcSinkOperation(config));
        }

        private async Task WaitForCdcInitialLoadAsync(IDocumentStore store, string configName, int timeoutMs = 60_000)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var process = db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
            if (process == null)
                throw new InvalidOperationException($"CDC Sink process '{configName}' not found");

            var completed = await Task.WhenAny(process.InitialLoadCompleted, Task.Delay(timeoutMs));
            if (completed != process.InitialLoadCompleted)
                throw new TimeoutException($"CDC Sink '{configName}' initial load did not complete within {timeoutMs}ms");

            await process.InitialLoadCompleted;
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

        // --- DTO classes ---

        private class Employee
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public string Department { get; set; }
        }

        private class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Person
        {
            public string Id { get; set; }
            public string FullName { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Customer { get; set; }
            public decimal Total { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public int LineNum { get; set; }
            public string Product { get; set; }
            public int Quantity { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Price { get; set; }
            public double TotalPrice { get; set; }
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
            public string InternalNotes { get; set; }
        }

        private class ArchivedOrder
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public bool Archived { get; set; }
            public string ArchivedAt { get; set; }
        }

        private class OrderWithStatus
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Status { get; set; }
        }

        private class InvoiceWithTotal
        {
            public string Id { get; set; }
            public string Customer { get; set; }
            public double TotalAmount { get; set; }
            public List<InvoiceLineWithAmount> Lines { get; set; }
        }

        private class InvoiceLineWithAmount
        {
            public int LineNum { get; set; }
            public string Description { get; set; }
            public decimal Amount { get; set; }
        }

        private class ConfigDoc
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public ConfigSettings Settings { get; set; }
            public List<string> Tags { get; set; }
            public string Description { get; set; }
        }

        private class ConfigSettings
        {
            public string Theme { get; set; }
            public NotificationSettings Notifications { get; set; }
        }

        private class NotificationSettings
        {
            public bool Email { get; set; }
            public bool Sms { get; set; }
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
                        Name = "Employees",
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
                        Name = "Products",
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
                        Name = "Items",
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
                        Name = "Notes",
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
                        Name = "Configs",
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
                        Name = "Orders",
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
                        Name = "People",
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
                        Name = "Products",
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
                        Name = "Orders",
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
                        Name = "Orders",
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
                        Name = "Customers",
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
                        Name = "Items",
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
                    total DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(id)
                )");

            ExecuteMySql(connectionString, "INSERT INTO customers (id, name) VALUES (42, 'Big Corp')");

            ExecuteMySql(connectionString, "INSERT INTO orders (id, customer_id, total) VALUES (1, 42, 150.00)");

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
        }
        [RavenFact(RavenTestCategory.Sinks, MySqlRequired = true, Skip = "Investigating: decimal delta computation mismatch in binlog path")]
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
                        Name = "Invoices",
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
    }
}
