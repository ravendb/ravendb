using System;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL;
using Raven.Server.SqlMigration;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL.SQL;

public class RavenDB_25832 : SqlAwareTestBase
{
    public RavenDB_25832(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TwoColumnItem
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }

    private const string SingleTableScript = @"
loadToItems({
    Name: this.Name
});";

    private const string TwoTableScript = @"
loadToItems_a({
    Name: this.Name
});
loadToItems_b({
    Value: this.Value
});";

    [RavenFact(RavenTestCategory.Etl, Requires = RavenServiceRequirement.NpgSql)]
    public async Task PostgresSqlEtl_RowConstraintViolation_OtherRowsStillLoad()
    {
        using (var store = GetDocumentStore())
        using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false))
        {
            CreateSingleTableSchema(connectionString);

            var etlDone = await SetUpEtl(store, "Items", new[] { "items" }, SingleTableScript, connectionString);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Name = "A" }, "items/1");
                await session.StoreAsync(new Item { Name = null }, "items/2");
                await session.StoreAsync(new Item { Name = "C" }, "items/3");
                await session.StoreAsync(new Item { Name = "D" }, "items/4");
                await session.SaveChangesAsync();
            }

            var stats = await etlDone.WaitForCompletion();

            Assert.Equal(3, stats.LoadSuccesses);
            Assert.Equal(1, stats.LoadErrors);
            AssertRowCount(connectionString, "items", expected: 3);
        }
    }

    [RavenFact(RavenTestCategory.Etl, Requires = RavenServiceRequirement.NpgSql)]
    public async Task PostgresSqlEtl_MultipleFailingRows_OtherRowsStillLoad()
    {
        using (var store = GetDocumentStore())
        using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false))
        {
            CreateSingleTableSchema(connectionString);

            var etlDone = await SetUpEtl(store, "Items", new[] { "items" }, SingleTableScript, connectionString);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Name = "A" }, "items/1");
                await session.StoreAsync(new Item { Name = null }, "items/2");
                await session.StoreAsync(new Item { Name = "C" }, "items/3");
                await session.StoreAsync(new Item { Name = null }, "items/4");
                await session.StoreAsync(new Item { Name = "E" }, "items/5");
                await session.StoreAsync(new Item { Name = null }, "items/6");
                await session.StoreAsync(new Item { Name = "G" }, "items/7");
                await session.SaveChangesAsync();
            }

            var stats = await etlDone.WaitForCompletion();

            Assert.Equal(4, stats.LoadSuccesses);
            Assert.Equal(3, stats.LoadErrors);
            AssertRowCount(connectionString, "items", expected: 4);
        }
    }

    [RavenFact(RavenTestCategory.Etl, Requires = RavenServiceRequirement.NpgSql)]
    public async Task PostgresSqlEtl_AllDocsInBatchFail_NextGoodDocStillLands()
    {
        using (var store = GetDocumentStore())
        using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false))
        {
            CreateSingleTableSchema(connectionString);

            var etlDone = await SetUpEtl(store, "Items", new[] { "items" }, SingleTableScript, connectionString);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item { Name = null }, "items/1");
                await session.StoreAsync(new Item { Name = null }, "items/2");
                await session.StoreAsync(new Item { Name = null }, "items/3");
                await session.StoreAsync(new Item { Name = null }, "items/4");
                await session.StoreAsync(new Item { Name = "OK" }, "items/5");
                await session.SaveChangesAsync();
            }

            var stats = await etlDone.WaitForCompletion();

            // every bad doc must be marked-and-skipped without poisoning the good one; ETL must not get stuck
            Assert.Equal(1, stats.LoadSuccesses);
            Assert.Equal(4, stats.LoadErrors);
            AssertRowCount(connectionString, "items", expected: 1);
        }
    }

    [RavenFact(RavenTestCategory.Etl, Requires = RavenServiceRequirement.NpgSql)]
    public async Task PostgresSqlEtl_DocFailingInOneTable_StillLandsInOtherTables()
    {
        using (var store = GetDocumentStore())
        using (WithSqlDatabase(MigrationProvider.NpgSQL, out var connectionString, out _, dataSet: null, includeData: false))
        {
            CreateTwoTableSchema(connectionString);

            var etlDone = await SetUpEtl(store, "TwoColumnItems", new[] { "items_a", "items_b" }, TwoTableScript, connectionString,
                collection: "TwoColumnItems");

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TwoColumnItem { Name = "A1", Value = "B1" }, "items/1");
                await session.StoreAsync(new TwoColumnItem { Name = "A2", Value = null }, "items/2");
                await session.StoreAsync(new TwoColumnItem { Name = "A3", Value = "B3" }, "items/3");
                await session.StoreAsync(new TwoColumnItem { Name = "A4", Value = "B4" }, "items/4");
                await session.SaveChangesAsync();
            }

            var stats = await etlDone.WaitForCompletion();

            // items/2 fails only on items_b (NOT NULL Value violation) - it still loads into items_a where its Name is valid
            Assert.Equal(7, stats.LoadSuccesses);
            Assert.Equal(1, stats.LoadErrors);
            AssertRowCount(connectionString, "items_a", expected: 4);
            AssertRowCount(connectionString, "items_b", expected: 3);
        }
    }

    private async Task<EtlCompletion> SetUpEtl(IDocumentStore store, string transformationName, string[] tableNames, string script,
        string connectionString, string collection = "Items")
    {
        var configurationName = transformationName + "_" + Guid.NewGuid();
        var connectionStringName = $"{transformationName}_{store.Database}";

        store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
        {
            Name = connectionStringName,
            FactoryName = "Npgsql",
            ConnectionString = connectionString
        }));

        var configuration = new SqlEtlConfiguration
        {
            Name = configurationName,
            ConnectionStringName = connectionStringName,
            Transforms =
            {
                new Transformation
                {
                    Name = transformationName,
                    Collections = { collection },
                    Script = script
                }
            }
        };
        foreach (var tableName in tableNames)
            configuration.SqlTables.Add(new SqlEtlTable { TableName = tableName, DocumentIdColumn = "Id" });

        store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(configuration));

        var database = await GetDatabase(store.Database);
        var completion = new EtlCompletion(configurationName, transformationName);
        database.EtlLoader.BatchCompleted += completion.OnBatchCompleted;
        return completion;
    }

    private sealed class EtlCompletion
    {
        private readonly string _configurationName;
        private readonly string _transformationName;
        private readonly AsyncManualResetEvent _done = new();
        private long _loadSuccesses;
        private long _loadErrors;

        public EtlCompletion(string configurationName, string transformationName)
        {
            _configurationName = configurationName;
            _transformationName = transformationName;
        }

        public void OnBatchCompleted((string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics) e)
        {
            if (e.ConfigurationName != _configurationName || e.TransformationName != _transformationName)
                return;

            if (e.Statistics.LoadSuccesses == 0)
                return;

            _loadSuccesses = e.Statistics.LoadSuccesses;
            _loadErrors = e.Statistics.LoadErrors;
            _done.Set();
        }

        public async Task<(long LoadSuccesses, long LoadErrors)> WaitForCompletion()
        {
            Assert.True(await _done.WaitAsync(TimeSpan.FromMinutes(1)),
                $"ETL batch did not complete in time. LoadSuccesses={_loadSuccesses}, LoadErrors={_loadErrors}");
            return (_loadSuccesses, _loadErrors);
        }
    }

    private static void CreateSingleTableSchema(string connectionString)
    {
        ExecuteSql(connectionString, @"
DROP TABLE IF EXISTS items;

CREATE TABLE items
(
    ""Id"" text NOT NULL PRIMARY KEY,
    ""Name"" text NOT NULL
);");
    }

    private static void CreateTwoTableSchema(string connectionString)
    {
        ExecuteSql(connectionString, @"
DROP TABLE IF EXISTS items_a;
DROP TABLE IF EXISTS items_b;

CREATE TABLE items_a
(
    ""Id"" text NOT NULL PRIMARY KEY,
    ""Name"" text
);

CREATE TABLE items_b
(
    ""Id"" text NOT NULL PRIMARY KEY,
    ""Value"" text NOT NULL
);");
    }

    private static void ExecuteSql(string connectionString, string sql)
    {
        using (var con = new NpgsqlConnection(connectionString))
        {
            con.Open();
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }
    }

    private static void AssertRowCount(string connectionString, string tableName, long expected)
    {
        using (var con = new NpgsqlConnection(connectionString))
        {
            con.Open();
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
                Assert.Equal(expected, cmd.ExecuteScalar());
            }
        }
    }
}
