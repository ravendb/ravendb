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
    /// <summary>
    /// Creates a single shared SQL Server database for all CDC tests in the collection.
    /// Enables CDC and creates a dummy table to bootstrap the SQL Agent capture/cleanup jobs
    /// (which takes ~6.5 seconds). Subsequent sp_cdc_enable_table calls are ~230ms.
    /// </summary>
    public class CdcSqlServerFixture : IDisposable
    {
        public string ConnectionString { get; }
        private readonly string _databaseName;

        public CdcSqlServerFixture()
        {
            _databaseName = "CdcTest_" + Guid.NewGuid();
            var masterCs = MsSqlConnectionString.Instance.VerifiedConnectionString.Value;
            ConnectionString = masterCs + $";Initial Catalog={_databaseName}";

            using (var conn = new SqlConnection(masterCs))
            {
                conn.Open();
                using var cmd = new SqlCommand($"CREATE DATABASE [{_databaseName}]", conn);
                cmd.CommandTimeout = 120;
                cmd.ExecuteNonQuery();
            }

            // Enable CDC + create a dummy table to pay the ~6.5s Agent job creation cost once
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand("EXEC sys.sp_cdc_enable_db", conn))
                {
                    cmd.CommandTimeout = 120;
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = new SqlCommand("CREATE TABLE _cdc_bootstrap (id INT PRIMARY KEY)", conn))
                    cmd.ExecuteNonQuery();

                using (var cmd = new SqlCommand(
                    "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'_cdc_bootstrap', @role_name = NULL", conn))
                {
                    cmd.CommandTimeout = 120;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public void Dispose()
        {
            try
            {
                var masterCs = MsSqlConnectionString.Instance.VerifiedConnectionString.Value;
                using var conn = new SqlConnection(masterCs);
                conn.Open();
                using var cmd = new SqlCommand(
                    $"ALTER DATABASE [{_databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_databaseName}]", conn);
                cmd.CommandTimeout = 120;
                cmd.ExecuteNonQuery();
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }

    [CollectionDefinition(nameof(CdcSinkSqlServerTests), DisableParallelization = true)]
    public class CdcSinkSqlServerTests : ICollectionFixture<CdcSqlServerFixture>;

    /// <summary>
    /// Integration tests for the SQL Server CDC Sink process.
    /// Mirrors the PostgreSQL tests in <see cref="CdcSinkPostgresIntegrationTests"/>.
    ///
    /// <para><b>Key differences from PostgreSQL CDC:</b></para>
    /// <list type="bullet">
    ///   <item>SQL Server CDC is poll-based (periodic queries), not push-based (streaming).</item>
    ///   <item>CDC must be explicitly enabled on the database (sp_cdc_enable_db) and on each table (sp_cdc_enable_table).</item>
    ///   <item>SQL Server CDC always captures all tracked columns on DELETE — no "REPLICA IDENTITY" concept.
    ///         This means embedded table deletes always have join columns available, unlike PostgreSQL
    ///         where you need composite PKs or REPLICA IDENTITY FULL.</item>
    ///   <item>SQL Server lacks native JSON, JSONB, array, tsvector, inet, and vector types.
    ///         JSON is typically stored as NVARCHAR(MAX).</item>
    ///   <item>Transactions are captured atomically via LSN ordering, similar to PostgreSQL's commit-based batching.</item>
    /// </list>
    /// </summary>
    [Collection(nameof(CdcSinkSqlServerTests))]
    public class CdcSinkSqlServerIntegrationTests : CdcSinkIntegrationTestBase
    {
        private readonly string _connectionString;

        public CdcSinkSqlServerIntegrationTests(ITestOutputHelper output, CdcSqlServerFixture fixture) : base(output)
        {
            _connectionString = fixture.ConnectionString;
        }

        /// <summary>
        /// Creates a unique schema for this test, named after the calling test method.
        /// Must be called at the start of each test before any SQL operations.
        /// </summary>
        private string CreateTestSchema([System.Runtime.CompilerServices.CallerMemberName] string testName = null)
        {
            ExecuteMsSql($"CREATE SCHEMA [{testName}]");
            return testName;
        }

        private void ExecuteMsSql(string sql)
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = 120;
            cmd.ExecuteNonQuery();
        }

        private void EnableCdcOnTable(string schema, string tableName)
        {
            ExecuteMsSql(
                $"EXEC sys.sp_cdc_enable_table @source_schema = N'{schema}', @source_name = N'{tableName}', @role_name = NULL");

            // SQL Server creates the capture instance asynchronously via SQL Agent.
            // Wait for it to become available before proceeding.
            WaitForCdcCaptureInstance(schema, tableName);
        }

        private void WaitForCdcCaptureInstance(string schema, string tableName)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 30_000)
            {
                using var connection = new SqlConnection(_connectionString);
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

        /// <summary>
        /// Enables CDC tracking on the specified tables.
        /// The database-level CDC and Agent jobs are already set up by the shared fixture.
        /// </summary>
        private void EnableCdcOnTables(params (string Schema, string Table)[] tables)
        {
            foreach (var (schema, table) in tables)
                EnableCdcOnTable(schema, table);
        }

        private SqlConnectionString SetupSqlConnectionString(IDocumentStore store, string name = "mssql-cdc-test")
        {
            var sqlCs = new SqlConnectionString
            {
                Name = name,
                FactoryName = "Microsoft.Data.SqlClient",
                ConnectionString = _connectionString
            };

            store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));
            return sqlCs;
        }

        /// <summary>
        /// Gets the CDC Sink process for the given store and config name.
        /// Throws if no process exists yet (call after AddCdcSink).
        /// </summary>
        private async Task<Raven.Server.Documents.CdcSink.CdcSinkProcess> GetCdcProcessAsync(IDocumentStore store, string configName)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            return db.CdcSinkLoader.Processes.FirstOrDefault(p => p.Name == configName);
        }

        /// <summary>
        /// Throws immediately if the CDC process has entered fallback mode due to an error.
        /// Call this in wait loops to fail fast instead of waiting for the full timeout.
        /// </summary>
        private static void ThrowOnProcessError(Raven.Server.Documents.CdcSink.CdcSinkProcess process)
        {
            if (process?.LastProcessException != null)
                throw new InvalidOperationException(
                    $"CDC Sink process '{process.Name}' failed: {process.LastProcessException.Message}",
                    process.LastProcessException);
        }

        /// <summary>
        /// Waits for the CDC Sink's initial load phase to complete.
        /// Fails immediately if the process encounters an error.
        /// </summary>
        private new async Task WaitForCdcInitialLoadAsync(IDocumentStore store, string configName, int timeoutMs = 60_000)
        {
            var process = await GetCdcProcessAsync(store, configName);
            if (process == null)
                throw new InvalidOperationException($"CDC Sink process '{configName}' not found");

            var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            process.ProcessError += ex => errorTcs.TrySetException(ex);

            var completed = await Task.WhenAny(process.InitialLoadCompleted, errorTcs.Task, Task.Delay(timeoutMs));

            if (completed == errorTcs.Task)
                await errorTcs.Task; // throws the process exception

            if (completed != process.InitialLoadCompleted)
            {
                ThrowOnProcessError(process);
                throw new TimeoutException($"CDC Sink '{configName}' initial load did not complete within {timeoutMs}ms");
            }

            await process.InitialLoadCompleted; // propagate any exception
        }

        private async Task<T> WaitForDocumentAsync<T>(IDocumentStore store, string docId, int timeoutMs = 30_000,
            Raven.Server.Documents.CdcSink.CdcSinkProcess process = null)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                ThrowOnProcessError(process);

                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<T>(docId);
                    if (doc != null)
                    {
                        return doc;
                    }
                }

                await Task.Delay(250);
            }

            ThrowOnProcessError(process);
            return null;
        }

        private async Task<bool> WaitForDocumentDeletionAsync(IDocumentStore store, string docId, int timeoutMs = 30_000,
            Raven.Server.Documents.CdcSink.CdcSinkProcess process = null)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                ThrowOnProcessError(process);

                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<object>(docId);
                    if (doc == null)
                    {
                        return true;
                    }
                }

                await Task.Delay(250);
            }

            ThrowOnProcessError(process);
            return false;
        }

        private async Task<int> WaitForDocumentCountAsync(IDocumentStore store, string collectionName, int expectedCount, int timeoutMs = 30_000,
            Raven.Server.Documents.CdcSink.CdcSinkProcess process = null)
        {
            var sw = Stopwatch.StartNew();
            int count = 0;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                ThrowOnProcessError(process);

                using (var session = store.OpenAsyncSession())
                {
                    count = await session.Query<dynamic>(collectionName: collectionName).CountAsync();
                    if (count >= expectedCount)
                    {
                        return count;
                    }
                }

                await Task.Delay(250);
            }

            ThrowOnProcessError(process);
            return count;
        }

        // ─────────────────────────────────────────────────────────────────────
        // Initial Load Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task InitialLoad_RootTable()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].products (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    price DECIMAL(12,2) NOT NULL
                )");

            ExecuteMsSql($@"
                INSERT INTO [{schema}].products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO [{schema}].products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO [{schema}].products (id, name, price) VALUES (3, 'Doohickey', 29.99);
                INSERT INTO [{schema}].products (id, name, price) VALUES (4, 'Precision', 123456789.01);");

            EnableCdcOnTables((schema, "products"));

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-initial-load",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task InitialLoad_WithColumnMapping()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].items (
                    product_id INT PRIMARY KEY IDENTITY(1,1),
                    product_name NVARCHAR(200) NOT NULL
                )");

            ExecuteMsSql($@"
                SET IDENTITY_INSERT [{schema}].items ON;
                INSERT INTO [{schema}].items (product_id, product_name) VALUES (1, 'Alpha');
                INSERT INTO [{schema}].items (product_id, product_name) VALUES (2, 'Beta');
                SET IDENTITY_INSERT [{schema}].items OFF;");

            EnableCdcOnTables((schema, "items"));

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-column-mapping",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = schema,
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

        // ─────────────────────────────────────────────────────────────────────
        // CDC Streaming Tests (Insert / Update / Delete)
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].events (
                    id INT PRIMARY KEY,
                    description NVARCHAR(200) NOT NULL
                )");

            EnableCdcOnTables((schema, "events"));

            ExecuteMsSql($@"INSERT INTO [{schema}].events (id, description) VALUES (1, 'Initial Event');");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Events",
                        SourceTableSchema = schema,
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

            var initialDoc = await WaitForDocumentAsync<Event>(store, "Events/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);

            // Insert a new row to be captured via CDC polling
            ExecuteMsSql($@"INSERT INTO [{schema}].events (id, description) VALUES (2, 'Streamed Event');");

            var newDoc = await WaitForDocumentAsync<Event>(store, "Events/2", timeoutMs: 60_000);
            Assert.NotNull(newDoc);
            Assert.Equal("Streamed Event", newDoc.Description);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Update()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].notes (
                    id INT PRIMARY KEY,
                    content NVARCHAR(500) NOT NULL
                )");

            EnableCdcOnTables((schema, "notes"));

            ExecuteMsSql($@"INSERT INTO [{schema}].notes (id, content) VALUES (1, 'Original Content');");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Notes",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($@"UPDATE [{schema}].notes SET content = 'Updated Content' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var updated = await session.LoadAsync<Note>("Notes/1");
                return updated?.Content;
            }, "Updated Content", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task CdcStreaming_Delete()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].records (
                    id INT PRIMARY KEY,
                    title NVARCHAR(200) NOT NULL
                )");

            EnableCdcOnTables((schema, "records"));

            // Note: Unlike PostgreSQL, no REPLICA IDENTITY FULL needed — SQL Server CDC
            // always captures all tracked columns on DELETE.
            ExecuteMsSql($@"
                INSERT INTO [{schema}].records (id, title) VALUES (1, 'To Be Deleted');
                INSERT INTO [{schema}].records (id, title) VALUES (2, 'To Keep');");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-cdc-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Records",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($@"DELETE FROM [{schema}].records WHERE id = 1;");

            var deleted = await WaitForDocumentDeletionAsync(store, "Records/1", timeoutMs: 60_000);
            Assert.True(deleted, "Document Records/1 should have been deleted after CDC DELETE");

            using (var session = store.OpenAsyncSession())
            {
                var kept = await session.LoadAsync<Record>("Records/2");
                Assert.NotNull(kept);
                Assert.Equal("To Keep", kept.Title);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Embedded Table Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                )");

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].order_lines (
                    id INT PRIMARY KEY,
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL
                )");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            ExecuteMsSql($@"
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (id, order_id, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO [{schema}].order_lines (id, order_id, product, quantity) VALUES (2, 1, 'Bananas', 3);");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-mssql-embedded-array",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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
                Assert.Contains("Bananas", products);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_CdcStreaming_Insert()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                );
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                )");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            ExecuteMsSql($"INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');");

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-cdc-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            // INSERT embedded rows via CDC polling (after initial load)
            ExecuteMsSql($"INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);");

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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_Delete()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                );
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                )");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            ExecuteMsSql($@"
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3);
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 3, 'Cherries', 7);");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            // Delete one embedded row via CDC
            ExecuteMsSql($"DELETE FROM [{schema}].order_lines WHERE order_id = 1 AND line_num = 2;");

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

        /// <summary>
        /// SQL Server CDC always captures all tracked columns on DELETE, so
        /// non-composite PK embedded tables work without any special configuration
        /// (unlike PostgreSQL which needs REPLICA IDENTITY FULL).
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_Delete_NonCompositePK()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                );
                CREATE TABLE [{schema}].order_lines (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL
                )");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            ExecuteMsSql($@"
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, product, quantity) VALUES (1, 'Apples', 5);
                INSERT INTO [{schema}].order_lines (order_id, product, quantity) VALUES (1, 'Bananas', 3);
                INSERT INTO [{schema}].order_lines (order_id, product, quantity) VALUES (1, 'Cherries', 7);");

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-noncomposite-delete",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            // Delete by simple PK — works because SQL Server CDC sends all columns on delete
            ExecuteMsSql($"DELETE FROM [{schema}].order_lines WHERE id = 2;");

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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_Update()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 1, 'Apples', 5);
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 2, 'Bananas', 3);");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($"UPDATE [{schema}].order_lines SET quantity = 99, product = 'Bananas (Updated)' WHERE order_id = 1 AND line_num = 2;");

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
        /// UPDATE that changes an embedded-table column listed in BOTH JoinColumns and
        /// PrimaryKeyColumns. SQL Server CDC emits a pre-image (op=3) and post-image (op=4)
        /// pair sharing the same (__$start_lsn, __$seqval). Reparent detection must pair
        /// them even though processor.GenerateDocumentId(values) — derived from the embedded
        /// PrimaryKeyColumns — produces different values for op=3 (old PK) and op=4 (new PK).
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_Reparent_WhenJoinColumnIsPartOfEmbeddedPK()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    quantity INT NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (2, 'Bob');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product, quantity) VALUES (1, 5, 'Apples', 7);");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-reparent-pk-includes-join",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
                                SourceTableName = "order_lines",
                                PropertyName = "Lines",
                                Type = CdcSinkRelationType.Array,
                                JoinColumns = new List<string> { "order_id" },
                                // Composite PK that *includes* the join column. With the pre-fix
                                // PK-derived dictionary key, op=3 ("OrderLines/1/5") and op=4
                                // ("OrderLines/2/5") never paired, no Delete from Orders/1 was
                                // emitted, and Orders/1.Lines kept a ghost entry.
                                PrimaryKeyColumns = new List<string> { "order_id", "line_num" },
                                Columns = new List<CdcColumnMapping>
                                {
                                    new CdcColumnMapping { Column = "order_id", Name = "OrderId" },
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

            // Initial load materializes the existing row in Orders/1.Lines.
            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order1 = await session.LoadAsync<Order>("Orders/1");
                return order1?.Lines?.Count ?? 0;
            }, 1, timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var order1 = await session.LoadAsync<Order>("Orders/1");
                Assert.Single(order1.Lines);
                Assert.Equal("Apples", order1.Lines[0].Product);
            }

            // Reparenting UPDATE: changes both the join column *and* a PK column.
            ExecuteMsSql($"UPDATE [{schema}].order_lines SET order_id = 2 WHERE order_id = 1 AND line_num = 5;");

            // Orders/1.Lines must lose the row (no ghost entry).
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
                Assert.Equal(5, order2.Lines[0].LineNum);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Patch Script Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PatchWithDollarRow()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].people (
                    id INT PRIMARY KEY,
                    first_name NVARCHAR(100) NOT NULL,
                    last_name NVARCHAR(100) NOT NULL
                );
                INSERT INTO [{schema}].people (id, first_name, last_name) VALUES (1, 'John', 'Doe');
                INSERT INTO [{schema}].people (id, first_name, last_name) VALUES (2, 'Jane', 'Smith');");

            EnableCdcOnTables((schema, "people"));

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-patch-dollar-row",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "People",
                        SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PatchScript_ModifiesMappedData()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].products (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    base_price DECIMAL(10,2) NOT NULL,
                    tax_rate DECIMAL(5,2) NOT NULL DEFAULT 0
                );
                INSERT INTO [{schema}].products (id, name, base_price, tax_rate) VALUES (1, 'Widget', 100.00, 0.20);");

            EnableCdcOnTables((schema, "products"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-patch-modifies",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PatchScript_CombinedRootAndEmbedded()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].invoices (id INT PRIMARY KEY, customer NVARCHAR(200) NOT NULL, discount_pct DECIMAL(5,2) DEFAULT 0);
                CREATE TABLE [{schema}].invoice_lines (
                    invoice_id INT NOT NULL REFERENCES [{schema}].invoices(id),
                    line_num INT NOT NULL,
                    description NVARCHAR(200) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num)
                );
                INSERT INTO [{schema}].invoices (id, customer, discount_pct) VALUES (1, 'Big Corp', 10.00);
                INSERT INTO [{schema}].invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00);
                INSERT INTO [{schema}].invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00);");

            EnableCdcOnTables((schema, "invoices"), (schema, "invoice_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-combined-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Invoices",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task MetadataExpires_ViaPatch()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].events (id INT PRIMARY KEY, title NVARCHAR(200) NOT NULL, expires_at DATETIME);
                INSERT INTO [{schema}].events (id, title, expires_at) VALUES (1, 'Flash Sale', '2099-12-31 23:59:59');");

            EnableCdcOnTables((schema, "events"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-expires-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Events",
                        SourceTableSchema = schema,
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

        // ─────────────────────────────────────────────────────────────────────
        // Linked Table Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task LinkedTable()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].customers (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL
                );
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_id INT NOT NULL REFERENCES [{schema}].customers(id),
                    total DECIMAL(10,2) NOT NULL
                );
                INSERT INTO [{schema}].customers (id, name) VALUES (42, 'Big Corp');
                INSERT INTO [{schema}].orders (id, customer_id, total) VALUES (1, 42, 150.00);");

            EnableCdcOnTables((schema, "customers"), (schema, "orders"));

            var sqlCs = SetupSqlConnectionString(store);

            var config = new CdcSinkConfiguration
            {
                Name = "test-linked-table",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Customers",
                        SourceTableSchema = schema,
                        SourceTableName = "customers",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" }
                        }
                    },
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

        // ─────────────────────────────────────────────────────────────────────
        // Three-Way Nesting Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task ThreeWayNesting()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].companies (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].departments (
                    company_id INT NOT NULL REFERENCES [{schema}].companies(id),
                    dept_id INT NOT NULL,
                    dept_name NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id)
                );
                CREATE TABLE [{schema}].employees (
                    company_id INT NOT NULL,
                    dept_id INT NOT NULL,
                    emp_id INT NOT NULL,
                    emp_name NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (company_id, dept_id, emp_id),
                    FOREIGN KEY (company_id, dept_id) REFERENCES [{schema}].departments(company_id, dept_id)
                );
                INSERT INTO [{schema}].companies (id, name) VALUES (1, 'Acme Corp');
                INSERT INTO [{schema}].departments (company_id, dept_id, dept_name) VALUES (1, 10, 'Engineering');
                INSERT INTO [{schema}].departments (company_id, dept_id, dept_name) VALUES (1, 20, 'Sales');
                INSERT INTO [{schema}].employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 1, 'Alice');
                INSERT INTO [{schema}].employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 10, 2, 'Bob');
                INSERT INTO [{schema}].employees (company_id, dept_id, emp_id, emp_name) VALUES (1, 20, 3, 'Charlie');");

            EnableCdcOnTables((schema, "companies"), (schema, "departments"), (schema, "employees"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-3-way-nesting",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Companies",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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
                                        SourceTableSchema = schema,
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

            // Delete one employee from Engineering
            ExecuteMsSql($"DELETE FROM [{schema}].employees WHERE company_id = 1 AND dept_id = 10 AND emp_id = 1;");

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
        }

        // ─────────────────────────────────────────────────────────────────────
        // Transaction Ordering Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task MultipleUpdates_SameRow_SameTransaction()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].counters (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL, value INT NOT NULL);
                INSERT INTO [{schema}].counters (id, name, value) VALUES (1, 'hits', 0);");

            EnableCdcOnTables((schema, "counters"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-multi-update",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Counters",
                        SourceTableSchema = schema,
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

            // Multiple updates in a single transaction — last one should win
            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                UPDATE [{schema}].counters SET value = 1 WHERE id = 1;
                UPDATE [{schema}].counters SET value = 2 WHERE id = 1;
                UPDATE [{schema}].counters SET value = 3 WHERE id = 1;
                COMMIT;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var counter = await session.LoadAsync<Counter>("Counters/1");
                return (int?)counter?.Value;
            }, 3, timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task Transaction_InsertUpdateDeleteInsert_SameRow()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"CREATE TABLE [{schema}].items (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL)");
            EnableCdcOnTables((schema, "items"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-insert-delete-insert",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                INSERT INTO [{schema}].items (id, name) VALUES (1, 'First');
                UPDATE [{schema}].items SET name = 'Second' WHERE id = 1;
                DELETE FROM [{schema}].items WHERE id = 1;
                INSERT INTO [{schema}].items (id, name) VALUES (1, 'Final');
                COMMIT;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var item = await session.LoadAsync<Item>("Items/1");
                return item?.Name;
            }, "Final", timeout: 60_000);
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task Transaction_MultipleDistinctRootDocuments()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"CREATE TABLE [{schema}].products (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL, price DECIMAL(10,2) NOT NULL)");
            EnableCdcOnTables((schema, "products"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-multi-root",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Products",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                INSERT INTO [{schema}].products (id, name, price) VALUES (1, 'Widget', 9.99);
                INSERT INTO [{schema}].products (id, name, price) VALUES (2, 'Gadget', 19.99);
                INSERT INTO [{schema}].products (id, name, price) VALUES (3, 'Doohickey', 29.99);
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
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task Transaction_MultipleRootAndEmbedded()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-and-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (2, 'Bob');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (2, 1, 'Cherries');
                COMMIT;");

            var count = await WaitForDocumentCountAsync(store, "Orders", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

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

        // ─────────────────────────────────────────────────────────────────────
        // Property Retention and Combined Update Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PropertyRetention_OnUpdate()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].customers (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL, email NVARCHAR(200));
                INSERT INTO [{schema}].customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

            EnableCdcOnTables((schema, "customers"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-property-retention",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Customers",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($"UPDATE [{schema}].customers SET name = 'Alice Updated' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var customer = await session.LoadAsync<Customer>("Customers/1");
                return customer?.Name;
            }, "Alice Updated", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var customer = await session.LoadAsync<Customer>("Customers/1");
                Assert.Equal("Alice Updated", customer.Name);
                Assert.Equal("VIP customer", customer.InternalNotes);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task UpdateParentAndEmbeddedTogether()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-update-parent-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                UPDATE [{schema}].orders SET customer_name = 'Alice Updated' WHERE id = 1;
                UPDATE [{schema}].order_lines SET product = 'Oranges' WHERE order_id = 1 AND line_num = 1;
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Grapes');
                COMMIT;");

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
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedArray_AddAndRemoveInSameTransaction()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-add-remove-txn",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                DELETE FROM [{schema}].order_lines WHERE order_id = 1 AND line_num = 1;
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries');
                COMMIT;");

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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task ChildBeforeParent()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            // Create tables without FK constraint so we can insert child before parent
            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL,
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-child-before-parent",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            // Insert child row FIRST
            ExecuteMsSql($"INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var order = await session.LoadAsync<Order>("Orders/1");
                if (order?.Lines == null) return 0;
                return order.Lines.Count;
            }, 1, timeout: 60_000);

            // Now insert the parent
            ExecuteMsSql($"INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');");

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

        // ─────────────────────────────────────────────────────────────────────
        // Binary / Attachment Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task BinaryColumn_RootAttachment()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].files (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL, content VARBINARY(MAX));
                INSERT INTO [{schema}].files (id, name, content) VALUES (1, 'readme.txt', 0x48656C6C6F20576F726C64);");

            EnableCdcOnTables((schema, "files"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-binary-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Files",
                        SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task TextAndBinaryColumns_AsAttachments()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].articles (
                    id INT PRIMARY KEY,
                    title NVARCHAR(200) NOT NULL,
                    body NVARCHAR(MAX),
                    summary NVARCHAR(1000),
                    thumbnail VARBINARY(MAX)
                );
                INSERT INTO [{schema}].articles (id, title, body, summary, thumbnail)
                VALUES (
                    1,
                    'Hello World',
                    'This is the full article body with lots of text content that should be stored as an attachment.',
                    'A brief summary of the article.',
                    0x89504E470D0A1A0A
                );");

            EnableCdcOnTables((schema, "articles"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-text-attachments",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Articles",
                        SourceTableSchema = schema,
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
        }

        // ─────────────────────────────────────────────────────────────────────
        // OnDelete Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PatchOnDelete_RootTable_ArchivePattern()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (
                    id INT PRIMARY KEY,
                    customer_name NVARCHAR(200) NOT NULL
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (2, 'Bob');");

            EnableCdcOnTables((schema, "orders"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-archive-root",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].orders WHERE id = 1;");

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

                var order2 = await session.LoadAsync<ArchivedOrder>("Orders/2");
                Assert.False(order2.Archived);
                Assert.Equal("Bob", order2.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Root_PatchOnly_AuditThenDelete()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (2, 'Bob');");

            EnableCdcOnTables((schema, "orders"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-patch-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].orders WHERE id = 1;");

            var deleted = await WaitForDocumentDeletionAsync(store, "Orders/1", timeoutMs: 60_000);
            Assert.True(deleted, "Document Orders/1 should be deleted (Patch runs but IgnoreDeletes is false)");

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<Order>("Orders/2");
                Assert.NotNull(order2);
                Assert.Equal("Bob", order2.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Root_IgnoreDeletesOnly_SilentIgnore()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');");

            EnableCdcOnTables((schema, "orders"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-ignore-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].orders WHERE id = 1;");
            ExecuteMsSql($"INSERT INTO [{schema}].orders (id, customer_name) VALUES (2, 'Bob');");

            var doc2 = await WaitForDocumentAsync<Order>(store, "Orders/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            using (var session = store.OpenAsyncSession())
            {
                var order1 = await session.LoadAsync<Order>("Orders/1");
                Assert.NotNull(order1);
                Assert.Equal("Alice", order1.CustomerName);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Root_ConditionalDelete()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL, status NVARCHAR(50) NOT NULL);
                INSERT INTO [{schema}].orders (id, customer_name, status) VALUES (1, 'Alice', 'Sent');
                INSERT INTO [{schema}].orders (id, customer_name, status) VALUES (2, 'Bob', 'Pending');");

            EnableCdcOnTables((schema, "orders"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-root-conditional",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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

            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                DELETE FROM [{schema}].orders WHERE id = 1;
                DELETE FROM [{schema}].orders WHERE id = 2;
                COMMIT;");

            var deleted = await WaitForDocumentDeletionAsync(store, "Orders/1", timeoutMs: 60_000);
            Assert.True(deleted, "Sent order (Orders/1) should be deleted by conditional del()");

            using (var session = store.OpenAsyncSession())
            {
                var order2 = await session.LoadAsync<OrderWithStatus>("Orders/2");
                Assert.NotNull(order2);
                Assert.Equal("Pending", order2.Status);
            }
        }

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task PatchOnDelete_EmbeddedTable()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-patchondelete-embedded",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].order_lines WHERE order_id = 1 AND line_num = 2;");

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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Embedded_IgnoreDeletesOnly()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-ignore-only",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].order_lines WHERE order_id = 1 AND line_num = 1;");
            ExecuteMsSql($"INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 3, 'Cherries');");

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

        // ─────────────────────────────────────────────────────────────────────
        // SQL Server Type Consistency Test
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Verifies that field representations in RavenDB documents are identical
        /// whether from initial load or CDC polling.
        ///
        /// <para><b>SQL Server vs PostgreSQL:</b> SQL Server lacks native JSON, array, tsvector,
        /// inet, and vector types. JSON is stored as NVARCHAR(MAX). Date types use DATETIME2/DATE
        /// instead of PostgreSQL's TIMESTAMP. UNIQUEIDENTIFIER maps to Guid.</para>
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task SqlServerTypeConsistency_InitialLoadVsCdcPolling()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].employees (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    birthday DATE NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    employee_id UNIQUEIDENTIFIER NOT NULL,
                    active BIT NOT NULL,
                    age INT NOT NULL,
                    score REAL NOT NULL
                );
                INSERT INTO [{schema}].employees (id, name, birthday, salary, employee_id, active, age, score)
                VALUES (1, 'Alice', '1990-06-15', 75000.50, 'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE', 1, 33, 4.5);");

            EnableCdcOnTables((schema, "employees"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-type-consistency",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "TypeEmployees",
                        SourceTableSchema = schema,
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

            var count = await WaitForDocumentCountAsync(store, "TypeEmployees", expectedCount: 1, timeoutMs: 60_000);
            Assert.Equal(1, count);

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

            ExecuteMsSql($@"UPDATE [{schema}].employees SET name = 'Alice Updated' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeStringFields>("TypeEmployees/1");
                return emp?.Name;
            }, "Alice Updated", timeout: 60_000);

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
        /// Verifies that datetime-family types (DATETIME2, DATETIMEOFFSET, TIME) survive
        /// both initial load (keyset pagination via ConvertStringToType) and CDC polling
        /// without type errors or precision loss.
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task DateTimeTypes_InitialLoadAndCdcPolling_Consistency()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].events (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    created_at DATETIME2(3) NOT NULL,
                    scheduled_at DATETIMEOFFSET NOT NULL,
                    duration TIME NOT NULL,
                    event_date DATE NOT NULL,
                    amount DECIMAL(18,6) NOT NULL
                );
                INSERT INTO [{schema}].events (id, name, created_at, scheduled_at, duration, event_date, amount)
                VALUES (1, 'Launch', '2025-03-15T14:30:45.123', '2025-03-15T14:30:45.1234567+05:30', '02:15:30.5000000', '2025-03-15', 123456.789012),
                       (2, 'Review', '2025-06-01T09:00:00.000', '2025-06-01T09:00:00.0000000-08:00', '01:00:00.0000000', '2025-06-01', 999999.999999);");

            EnableCdcOnTables((schema, "events"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-datetime-types",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Events",
                        SourceTableSchema = schema,
                        SourceTableName = "events",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "created_at", Name = "CreatedAt" },
                            new CdcColumnMapping { Column = "scheduled_at", Name = "ScheduledAt" },
                            new CdcColumnMapping { Column = "duration", Name = "Duration" },
                            new CdcColumnMapping { Column = "event_date", Name = "EventDate" },
                            new CdcColumnMapping { Column = "amount", Name = "Amount" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);

            // Wait for initial load (exercises ConvertStringToType with datetime types)
            var count = await WaitForDocumentCountAsync(store, "Events", expectedCount: 2, timeoutMs: 60_000);
            Assert.Equal(2, count);

            // Capture initial load representations
            string initialCreatedAt, initialScheduledAt, initialDuration, initialEventDate, initialAmount;
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<EventStringFields>("Events/1");
                Assert.NotNull(doc);
                Assert.Equal("Launch", doc.Name);
                initialCreatedAt = doc.CreatedAt;
                initialScheduledAt = doc.ScheduledAt;
                initialDuration = doc.Duration;
                initialEventDate = doc.EventDate;
                initialAmount = doc.Amount;
            }

            // Update via CDC — only change name to trigger a CDC row with all columns
            ExecuteMsSql($@"UPDATE [{schema}].events SET name = 'Launch v2' WHERE id = 1");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<EventStringFields>("Events/1");
                return doc?.Name;
            }, "Launch v2", timeout: 60_000);

            // Verify datetime representations are identical between initial load and CDC
            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<EventStringFields>("Events/1");
                Assert.NotNull(doc);
                Assert.Equal("Launch v2", doc.Name);
                Assert.Equal(initialCreatedAt, doc.CreatedAt);
                Assert.Equal(initialScheduledAt, doc.ScheduledAt);
                Assert.Equal(initialDuration, doc.Duration);
                Assert.Equal(initialEventDate, doc.EventDate);
                Assert.Equal(initialAmount, doc.Amount);
            }
        }

        private class EventStringFields
        {
            public string Name { get; set; }
            public string CreatedAt { get; set; }
            public string ScheduledAt { get; set; }
            public string Duration { get; set; }
            public string EventDate { get; set; }
            public string Amount { get; set; }
        }

        /// <summary>
        /// Verifies that NVARCHAR(MAX) columns declared with CdcColumnType.Json are stored as
        /// native JSON objects/arrays in the RavenDB document (not as escaped strings).
        /// Tests both initial load and CDC polling to confirm both paths handle
        /// JSON columns identically.
        ///
        /// <para><b>SQL Server vs PostgreSQL:</b> SQL Server has no native JSON/JSONB column type
        /// (prior to SQL Server 2022 JSON type). JSON data is stored in NVARCHAR(MAX) columns
        /// and the CdcColumnType.Json mapping tells the CDC sink to parse the string value as JSON.</para>
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task JsonColumns_StoredAsNativeJsonObjects()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].configs (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    settings NVARCHAR(MAX) NOT NULL,
                    tags NVARCHAR(MAX),
                    description NVARCHAR(MAX)
                );
                INSERT INTO [{schema}].configs (id, name, settings, tags, description)
                VALUES (
                    1,
                    'AppConfig',
                    '{{""theme"": ""dark"", ""notifications"": {{""email"": true, ""sms"": false}}}}',
                    '[""production"", ""v2""]',
                    'Main application configuration'
                );");

            EnableCdcOnTables((schema, "configs"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-json-columns",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Configs",
                        SourceTableSchema = schema,
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

            // CDC polling: update the JSON columns
            ExecuteMsSql($@"
                UPDATE [{schema}].configs SET
                    settings = '{{""theme"": ""light"", ""notifications"": {{""email"": false, ""sms"": true}}, ""newField"": 42}}',
                    tags = '[""staging"", ""v3"", ""beta""]'
                WHERE id = 1;");

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

        // ─────────────────────────────────────────────────────────────────────
        // Complex Types Test (SQL Server specific)
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Verifies that SQL Server complex types (JSON, GEOMETRY, UNIQUEIDENTIFIER, XML,
        /// DATETIMEOFFSET, HIERARCHYID) are handled correctly in both initial load and CDC polling paths.
        ///
        /// <para><b>SQL Server vs PostgreSQL:</b></para>
        /// <list type="bullet">
        ///   <item>SQL Server 2022+ has a native JSON type (mapped similarly to NVARCHAR(MAX) for CDC).</item>
        ///   <item>GEOMETRY/GEOGRAPHY replace PostGIS spatial types.</item>
        ///   <item>No equivalent to tsvector, inet, or text[] — these are SQL Server gaps.</item>
        ///   <item>UNIQUEIDENTIFIER → Guid → string, XML → string.</item>
        /// </list>
        /// </summary>
        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task ComplexTypes_Json_Geometry_Guid_Xml()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].complex_docs (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    metadata NVARCHAR(MAX),
                    settings NVARCHAR(MAX),
                    location GEOMETRY,
                    doc_guid UNIQUEIDENTIFIER,
                    extra_xml XML,
                    big_number BIGINT,
                    created_at DATETIMEOFFSET
                );
                INSERT INTO [{schema}].complex_docs (id, name, metadata, settings, location, doc_guid, extra_xml, big_number, created_at)
                VALUES (
                    1,
                    'TestDoc',
                    '{{""key"": ""value"", ""nested"": {{""a"": 1}}}}',
                    '{{""theme"": ""dark"", ""lang"": ""en""}}',
                    geometry::Point(47.6062, -122.3321, 4326),
                    'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE',
                    '<config><setting name=""mode"">test</setting></config>',
                    9223372036854775807,
                    '2026-01-15T10:30:00+02:00'
                );");

            EnableCdcOnTables((schema, "complex_docs"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-complex-types",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "ComplexDocs",
                        SourceTableSchema = schema,
                        SourceTableName = "complex_docs",
                        PrimaryKeyColumns = new List<string> { "id" },
                        Columns = new List<CdcColumnMapping>
                        {
                            new CdcColumnMapping { Column = "id", Name = "DbId" },
                            new CdcColumnMapping { Column = "name", Name = "Name" },
                            new CdcColumnMapping { Column = "metadata", Name = "Metadata" },
                            new CdcColumnMapping { Column = "settings", Name = "Settings" },
                            new CdcColumnMapping { Column = "location", Name = "Location" },
                            new CdcColumnMapping { Column = "doc_guid", Name = "DocGuid" },
                            new CdcColumnMapping { Column = "extra_xml", Name = "ExtraXml" },
                            new CdcColumnMapping { Column = "big_number", Name = "BigNumber" },
                            new CdcColumnMapping { Column = "created_at", Name = "CreatedAt" }
                        }
                    }
                }
            };

            AddCdcSink(store, config);
            await WaitForCdcInitialLoadAsync(store, "test-complex-types");

            var initialDoc = await WaitForDocumentAsync<ComplexDoc>(store, "ComplexDocs/1", timeoutMs: 60_000);
            Assert.NotNull(initialDoc);
            Assert.Equal("TestDoc", initialDoc.Name);

            // JSON stored as NVARCHAR(MAX) should arrive as string
            Assert.NotNull(initialDoc.Metadata);
            Assert.Contains("key", initialDoc.Metadata);
            Assert.Contains("value", initialDoc.Metadata);

            Assert.NotNull(initialDoc.Settings);
            Assert.Contains("theme", initialDoc.Settings);
            Assert.Contains("dark", initialDoc.Settings);

            // GEOMETRY → WKT string
            Assert.NotNull(initialDoc.Location);
            Assert.Contains("POINT", initialDoc.Location.ToUpperInvariant());

            // UNIQUEIDENTIFIER → string
            Assert.NotNull(initialDoc.DocGuid);
            Assert.Contains("aaaa", initialDoc.DocGuid.ToLowerInvariant());

            // XML → string
            Assert.NotNull(initialDoc.ExtraXml);
            Assert.Contains("config", initialDoc.ExtraXml);

            // BIGINT → long
            Assert.NotNull(initialDoc.BigNumber);

            // DATETIMEOFFSET → string representation
            Assert.NotNull(initialDoc.CreatedAt);

            // Capture initial values for comparison after CDC update
            var initialGuid = initialDoc.DocGuid;

            // --- CDC polling: update complex columns ---
            ExecuteMsSql($@"
                UPDATE [{schema}].complex_docs SET
                    metadata = '{{""key"": ""updated"", ""extra"": true}}',
                    settings = '{{""theme"": ""light"", ""lang"": ""fr""}}',
                    location = geometry::Point(48.8566, 2.3522, 4326),
                    doc_guid = '11111111-2222-3333-4444-555555555555',
                    extra_xml = '<config><setting name=""mode"">production</setting></config>',
                    big_number = 42,
                    created_at = '2026-06-01T14:00:00+00:00'
                WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var doc2 = await session.LoadAsync<ComplexDoc>("ComplexDocs/1");
                return doc2?.Settings;
            }, "{\"theme\": \"light\", \"lang\": \"fr\"}", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var updated = await session.LoadAsync<ComplexDoc>("ComplexDocs/1");
                Assert.NotNull(updated);

                Assert.Contains("updated", updated.Metadata);
                Assert.Contains("light", updated.Settings);
                Assert.Contains("POINT", updated.Location.ToUpperInvariant());
                Assert.Contains("production", updated.ExtraXml);
                Assert.NotEqual(initialGuid, updated.DocGuid);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Embedded Attachment Tests
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task BinaryColumn_EmbeddedAttachment()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].albums (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].photos (
                    album_id INT NOT NULL REFERENCES [{schema}].albums(id),
                    photo_num INT NOT NULL,
                    title NVARCHAR(200) NOT NULL,
                    thumbnail VARBINARY(MAX),
                    PRIMARY KEY (album_id, photo_num)
                );
                INSERT INTO [{schema}].albums (id, name) VALUES (1, 'Vacation');
                INSERT INTO [{schema}].photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', 0x89504E47);
                INSERT INTO [{schema}].photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', 0xFFD8FFE0);");

            EnableCdcOnTables((schema, "albums"), (schema, "photos"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-embedded-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Albums",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task DeleteAttachment_OnEmbeddedDelete()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].albums (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].photos (
                    album_id INT NOT NULL REFERENCES [{schema}].albums(id),
                    photo_num INT NOT NULL,
                    title NVARCHAR(200) NOT NULL,
                    thumbnail VARBINARY(MAX),
                    PRIMARY KEY (album_id, photo_num)
                );
                INSERT INTO [{schema}].albums (id, name) VALUES (1, 'Vacation');
                INSERT INTO [{schema}].photos (album_id, photo_num, title, thumbnail) VALUES (1, 1, 'Beach', 0x89504E47);
                INSERT INTO [{schema}].photos (album_id, photo_num, title, thumbnail) VALUES (1, 2, 'Mountain', 0xFFD8FFE0);");

            EnableCdcOnTables((schema, "albums"), (schema, "photos"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-delete-attachment",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Albums",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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
            ExecuteMsSql($"DELETE FROM [{schema}].photos WHERE album_id = 1 AND photo_num = 1;");

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

        // ─────────────────────────────────────────────────────────────────────
        // Embedded Patch Delta Computation Test
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EmbeddedPatch_OldRowData_DeltaComputation()
        {
            // Tests that $old is available in embedded patches for delta computations.
            // When an embedded line item's amount changes, the parent's TotalAmount
            // is adjusted by the delta (new - old), not the absolute new value.
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].invoices (id INT PRIMARY KEY, customer NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].invoice_lines (
                    invoice_id INT NOT NULL REFERENCES [{schema}].invoices(id),
                    line_num INT NOT NULL,
                    description NVARCHAR(200) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    PRIMARY KEY (invoice_id, line_num)
                );
                INSERT INTO [{schema}].invoices (id, customer) VALUES (1, 'Acme');
                INSERT INTO [{schema}].invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 1, 'Service A', 100.00);
                INSERT INTO [{schema}].invoice_lines (invoice_id, line_num, description, amount) VALUES (1, 2, 'Service B', 200.00);");

            EnableCdcOnTables((schema, "invoices"), (schema, "invoice_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-old-row-delta",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Invoices",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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
            ExecuteMsSql($"UPDATE [{schema}].invoice_lines SET amount = 150.00 WHERE invoice_id = 1 AND line_num = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var inv = await session.LoadAsync<InvoiceWithTotal>("Invoices/1");
                return inv?.TotalAmount;
            }, 350.0, timeout: 60_000);

            // Update line 2: amount changes from 200 to 50
            // Delta = 50 - 200 = -150, so TotalAmount should go from 350 to 200
            ExecuteMsSql($"UPDATE [{schema}].invoice_lines SET amount = 50.00 WHERE invoice_id = 1 AND line_num = 2;");

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

        // ─────────────────────────────────────────────────────────────────────
        // Audit Trail Test
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task Patch_AuditTrail_InsertUpdateDeleteInsertUpdate()
        {
            // Verifies that Patch and OnDelete.Patch record a full audit trail of
            // operations via put(), and that the sequence matches the SQL transaction order:
            // INSERT → UPDATE → DELETE → INSERT → UPDATE
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"CREATE TABLE [{schema}].items (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL)");
            EnableCdcOnTables((schema, "items"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-audit-trail",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = schema,
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
            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                INSERT INTO [{schema}].items (id, name) VALUES (1, 'Alpha');
                UPDATE [{schema}].items SET name = 'Beta' WHERE id = 1;
                DELETE FROM [{schema}].items WHERE id = 1;
                INSERT INTO [{schema}].items (id, name) VALUES (1, 'Gamma');
                UPDATE [{schema}].items SET name = 'Delta' WHERE id = 1;
                COMMIT;");

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

        // ─────────────────────────────────────────────────────────────────────
        // Embedded OnDelete with IgnoreDeletes + Patch (Archive pattern)
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Embedded_PatchAndIgnoreDeletes_Archive()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].orders (id INT PRIMARY KEY, customer_name NVARCHAR(200) NOT NULL);
                CREATE TABLE [{schema}].order_lines (
                    order_id INT NOT NULL REFERENCES [{schema}].orders(id),
                    line_num INT NOT NULL,
                    product NVARCHAR(200) NOT NULL,
                    PRIMARY KEY (order_id, line_num)
                );
                INSERT INTO [{schema}].orders (id, customer_name) VALUES (1, 'Alice');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 1, 'Apples');
                INSERT INTO [{schema}].order_lines (order_id, line_num, product) VALUES (1, 2, 'Bananas');");

            EnableCdcOnTables((schema, "orders"), (schema, "order_lines"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-emb-archive",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Orders",
                        SourceTableSchema = schema,
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
                                SourceTableSchema = schema,
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

            ExecuteMsSql($"DELETE FROM [{schema}].order_lines WHERE order_id = 1 AND line_num = 2;");

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

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task OnDelete_Root_InsertThenDeleteInSameTransaction()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"CREATE TABLE [{schema}].items (id INT PRIMARY KEY, name NVARCHAR(200) NOT NULL)");
            EnableCdcOnTables((schema, "items"));

            var sqlCs = SetupSqlConnectionString(store);
            var config = new CdcSinkConfiguration
            {
                Name = "test-insert-delete-patch",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Items",
                        SourceTableSchema = schema,
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
            ExecuteMsSql($@"
                BEGIN TRANSACTION;
                INSERT INTO [{schema}].items (id, name) VALUES (1, 'Ephemeral');
                DELETE FROM [{schema}].items WHERE id = 1;
                COMMIT;");

            // Insert another item to prove CDC advanced past the transaction
            ExecuteMsSql($"INSERT INTO [{schema}].items (id, name) VALUES (2, 'Permanent');");
            var doc2 = await WaitForDocumentAsync<Item>(store, "Items/2", timeoutMs: 60_000);
            Assert.NotNull(doc2);

            // Item 1 should be deleted (Patch ran but IgnoreDeletes is false)
            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>("Items/1");
                Assert.Null(item1);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Edit Task Test
        // ─────────────────────────────────────────────────────────────────────

        [RavenFact(RavenTestCategory.Sinks, MsSqlRequired = true, MsSqlCdcRequired = true)]
        public async Task EditTask_AddSecondTable_InitialLoadAndCdcWorkForBoth()
        {
            using var store = GetDocumentStore();
            var schema = CreateTestSchema();

            ExecuteMsSql($@"
                CREATE TABLE [{schema}].employees (
                    id INT PRIMARY KEY,
                    name NVARCHAR(200) NOT NULL,
                    department NVARCHAR(200)
                );
                CREATE TABLE [{schema}].cars (
                    id INT PRIMARY KEY,
                    make NVARCHAR(200) NOT NULL,
                    model NVARCHAR(200) NOT NULL
                );
                INSERT INTO [{schema}].employees (id, name, department) VALUES (1, 'Alice', 'Engineering');
                INSERT INTO [{schema}].employees (id, name, department) VALUES (2, 'Bob', 'Marketing');
                INSERT INTO [{schema}].cars (id, make, model) VALUES (1, 'Toyota', 'Camry');
                INSERT INTO [{schema}].cars (id, make, model) VALUES (2, 'Honda', 'Civic');");

            EnableCdcOnTables((schema, "employees"), (schema, "cars"));

            var sqlCs = SetupSqlConnectionString(store);

            // Phase 1: Create CDC task with Employees only
            var config = new CdcSinkConfiguration
            {
                Name = "test-edit-add-table",
                ConnectionStringName = sqlCs.Name,
                Tables = new List<CdcSinkTableConfig>
                {
                    new CdcSinkTableConfig
                    {
                        CollectionName = "Employees",
                        SourceTableSchema = schema,
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

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Name;
            }, "Alice", timeout: 60_000);

            using (var session = store.OpenAsyncSession())
            {
                var car = await session.LoadAsync<Car>("Cars/1");
                Assert.Null(car);
            }

            // Phase 2: Verify CDC polling for employees
            ExecuteMsSql($"UPDATE [{schema}].employees SET department = 'Management' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Department;
            }, "Management", timeout: 60_000);

            // Phase 3: Edit the task to add Cars table
            config.TaskId = addResult.TaskId;
            config.Tables.Add(new CdcSinkTableConfig
            {
                CollectionName = "Cars",
                SourceTableSchema = schema,
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
                if (process == null) return false;
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

            // Phase 4: Verify CDC polling works for Cars
            ExecuteMsSql($"UPDATE [{schema}].cars SET model = 'Accord' WHERE id = 2;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var car = await session.LoadAsync<Car>("Cars/2");
                return car?.Model;
            }, "Accord", timeout: 60_000);

            // Phase 5: Verify CDC polling still works for Employees
            ExecuteMsSql($"UPDATE [{schema}].employees SET name = 'Alice Smith' WHERE id = 1;");

            await AssertWaitForValueAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var emp = await session.LoadAsync<EmployeeRecord>("Employees/1");
                return emp?.Name;
            }, "Alice Smith", timeout: 60_000);
        }

        // ─────────────────────────────────────────────────────────────────────
        // SQL Server-specific DTO (different complex type fields)
        // ─────────────────────────────────────────────────────────────────────

        private class ComplexDoc
        {
            public int DbId { get; set; }
            public string Name { get; set; }
            public string Metadata { get; set; }
            public string Settings { get; set; }
            public string Location { get; set; }
            public string DocGuid { get; set; }
            public string ExtraXml { get; set; }
            public string BigNumber { get; set; }
            public string CreatedAt { get; set; }
        }
    }
}
