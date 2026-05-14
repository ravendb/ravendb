using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Test;
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
                Connection = new Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString
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
                Connection = new Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString
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
                Connection = new Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString
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
                Connection = new Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString
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
    }
}
