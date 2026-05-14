using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.CdcSink
{
    /// <summary>
    /// Exercises the <see cref="TestCdcSinkMappingOperation"/> client API end-to-end:
    /// the .NET client builds the request, hits POST /admin/cdc-sink/test, deserializes
    /// the response into typed DTOs. Complements the direct <c>CdcSinkTestRunner.Run</c>
    /// tests in <see cref="CdcSinkPostgresTestMappingTests"/>.
    /// </summary>
    [Collection(nameof(CdcSinkPostgresTests))]
    public class CdcSinkPostgresTestMappingClientApiTests : CdcSinkIntegrationTestBase
    {
        public CdcSinkPostgresTestMappingClientApiTests(ITestOutputHelper output) : base(output)
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
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200), email VARCHAR(200));
                INSERT INTO lecturers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
                INSERT INTO lecturers (id, name, email) VALUES (2, 'Bob',   'bob@example.com');");

            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                    new() { Column = "email", Name = "Email" },
                },
                Patch = "this.Greeting = 'hi ' + $row.name;",
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-test",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 2,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.NotNull(result);
            Assert.Empty(result.Errors);
            Assert.Equal(2, result.Results.Count);

            var first = result.Results[0];
            Assert.Equal("Lecturers/1", first.DocumentId);
            Assert.False(first.WouldDelete);
            Assert.Null(first.Error);
            Assert.NotNull(first.Document);
            Assert.Contains("\"Name\":\"Alice\"", first.Document);
            Assert.Contains("\"Greeting\":\"hi Alice\"", first.Document);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_DeleteMode_PreservesIgnoreDeletesFlag()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
                OnDelete = new CdcSinkOnDeleteConfig { IgnoreDeletes = true },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-delete",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Delete,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.False(row.WouldDelete);
            Assert.True(row.IgnoreDeletes);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_MaxRowsAboveCap_RejectedWithStructuredError()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-cap",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = int.MaxValue,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains(nameof(TestCdcSinkMappingRequest.MaxRows), error);
            Assert.Contains("between 1 and 5,000", error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_RowFetchConnectionFailure_ReturnsGenericErrorWithoutDriverDetails()
        {
            using var store = GetDocumentStore();

            // Point at a host that won't resolve so FetchRowsAsync throws inside the handler.
            // The driver's raw exception text contains the host and internal Npgsql codes; the
            // response body must echo NEITHER and instead surface the generic operator message.
            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-fetch-failure",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = "Host=cdc-test-no-such-host.invalid;Database=postgres;User Id=postgres;Password=x;Timeout=2;Command Timeout=2",
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains("Failed to fetch rows from the source database", error);
            Assert.DoesNotContain("cdc-test-no-such-host", error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_DuplicateTableInConfig_SurfacesAmbiguityError()
        {
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            // Two table entries that differ only by case match the same selector (case-insensitive).
            // Without the duplicate-detection guard, FirstOrDefault would silently pick one and run.
            // The guard fails fast with a structured Errors entry.
            CdcSinkTableConfig MakeTable(string sourceName, string collectionName) => new()
            {
                CollectionName = collectionName,
                SourceTableSchema = "public",
                SourceTableName = sourceName,
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-dup",
                    Tables = new List<CdcSinkTableConfig>
                    {
                        MakeTable("lecturers", "Lecturers"),
                        MakeTable("Lecturers", "LecturersUpper"),
                    },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains("2 tables matching", error);
            Assert.Contains("case-insensitive", error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task InlineConnectionWinsOverNamedConnectionString()
        {
            // Documents the inline-vs-named precedence in CdcSinkHandler.ResolveSqlConnection:
            // when both are provided, the inline credentials take precedence (matches the comment
            // on CdcSinkSchemaRequest.Connection). Studio's Task Creation view relies on this so
            // the user can test against credentials they haven't saved yet.
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-precedence",
                    // Bogus name; if the resolver fell back to the named lookup it would fail.
                    // The inline Connection below must win.
                    ConnectionStringName = "does-not-exist-on-this-database",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.Equal("Lecturers/1", row.DocumentId);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task NamedConnectionNotFound_ReturnsStructuredError()
        {
            // No inline Connection + a ConnectionStringName that doesn't exist in
            // databaseRecord.SqlConnectionStrings -> resolver throws InvalidOperationException
            // which the handler catches and surfaces as a structured Errors entry (not a 500).
            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-missing-name",
                    ConnectionStringName = "does-not-exist-on-this-database",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                // Connection deliberately null.
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains("does-not-exist-on-this-database", error);
            Assert.Contains("not found", error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_PatchWithSyntaxError_ReturnsStructuredError()
        {
            // A malformed Patch script (Jint compile error) used to bubble through
            // CdcSinkTestRunner.Run as an unhandled exception, surfacing as HTTP 500.
            // Studio expects the same shape as other validation failures: HTTP 200
            // with a populated TestCdcSinkMappingResult.Errors.
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
                // Deliberately invalid JS — assignment with no right-hand side.
                Patch = "this.X = ;",
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-bad-patch",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains("failed to compile", error);
        }

        [RavenFact(RavenTestCategory.Sinks, NpgSqlRequired = true)]
        public async Task ClientOperation_ConfigWithEmptySchema_MatchesProviderDefault()
        {
            // A saved CDC config may leave SourceTableSchema null/empty for runtime reasons:
            // the CDC runtime resolves it to the provider default (Postgres="public", SQL
            // Server="dbo", MySQL=database name). Studio's schema discovery endpoint, however,
            // returns case-preserving names with the explicit schema ("public"), so the user's
            // /admin/cdc-sink/test request supplies "public" even though the saved config has
            // empty schema. Before the fix, the handler's match expression compared
            // (t.SourceTableSchema ?? "") to (request.SourceTableSchema ?? "") and failed when
            // either side was empty.
            using var teardown = WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false);

            ExecuteNpgSql(connectionString, @"
                CREATE TABLE lecturers (id SERIAL PRIMARY KEY, name VARCHAR(200));
                INSERT INTO lecturers (id, name) VALUES (1, 'Alice');");

            using var store = GetDocumentStore();

            // Note: SourceTableSchema deliberately left null on both the table config AND below
            // is mirrored by ConfigurationName in CdcSinkConfiguration so the runtime would
            // fall back to provider-default. The fix must accept "public" from the request.
            var table = new CdcSinkTableConfig
            {
                CollectionName = "Lecturers",
                SourceTableSchema = null,
                SourceTableName = "lecturers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                    new() { Column = "name", Name = "Name" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-default-schema",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Npgsql",
                    ConnectionString = connectionString,
                },
                SourceTableSchema = "public",
                SourceTableName = "lecturers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Errors);
            var row = Assert.Single(result.Results);
            Assert.Equal("Lecturers/1", row.DocumentId);
            Assert.Contains("\"Name\":\"Alice\"", row.Document);
        }

        [RavenFact(RavenTestCategory.Sinks)]
        public async Task ClientOperation_UnsupportedProvider_RejectedBeforeAnySqlIsRun()
        {
            // DatabaseDriverDispatcher accepts Oracle (SQL Migration supports it), but the CDC
            // sink does not. The /verify and /schema endpoints already gate on the narrower
            // CDC-supported provider list; ensure /test does the same so an admin can't preview
            // a CDC config the rest of the subsystem will refuse to run. Driver isn't actually
            // loaded — the early gate fires before any SQL connection is attempted, so no
            // Oracle infrastructure is required for this test.
            using var store = GetDocumentStore();

            var table = new CdcSinkTableConfig
            {
                CollectionName = "Customers",
                SourceTableSchema = "dbo",
                SourceTableName = "customers",
                PrimaryKeyColumns = new List<string> { "id" },
                Columns = new List<CdcColumnMapping>
                {
                    new() { Column = "id", Name = "DbId" },
                },
            };

            var request = new TestCdcSinkMappingRequest
            {
                Configuration = new CdcSinkConfiguration
                {
                    Name = "client-api-unsupported-provider",
                    Tables = new List<CdcSinkTableConfig> { table },
                },
                Connection = new SqlConnectionString
                {
                    FactoryName = "Oracle.ManagedDataAccess.Client",
                    ConnectionString = "Data Source=irrelevant;User Id=x;Password=y;",
                },
                SourceTableSchema = "dbo",
                SourceTableName = "customers",
                RowSelector = TestCdcSinkRowSelector.First,
                Operation = TestCdcSinkOperation.Upsert,
                MaxRows = 1,
            };

            var result = await store.Maintenance.SendAsync(new TestCdcSinkMappingOperation(request));

            Assert.Empty(result.Results);
            var error = Assert.Single(result.Errors);
            Assert.Contains("Oracle.ManagedDataAccess.Client", error);
            Assert.Contains("does not support provider", error);
        }
    }
}
