using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Issues;

public class RavenDB_27616 : RavenTestBase
{
    public RavenDB_27616(ITestOutputHelper output) : base(output)
    {
        DoNotReuseServer();
    }

    private static SqlConnectionString CreateSqlConnectionString(string name) => new()
    {
        Name = name,
        FactoryName = "Microsoft.Data.SqlClient",
        ConnectionString = "Server=localhost;Database=TestDb;User Id=sa;Password=pass;"
    };

    private static CdcSinkConfiguration CreateCdcSink(string name, string connectionStringName) => new()
    {
        Name = name,
        ConnectionStringName = connectionStringName,
        Tables = new List<CdcSinkTableConfig>
        {
            new()
            {
                CollectionName = "Orders",
                SourceTableSchema = "public",
                SourceTableName = "orders",
                Columns = new List<CdcColumnMapping> { new() { Column = "order_id", Name = "OrderId" } },
                PrimaryKeyColumns = new List<string> { "order_id" }
            }
        }
    };

    private static AiConnectionString CreateAiConnectionString(string name)
    {
        var cs = new AiConnectionString
        {
            Name = name,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings { ApiKey = "fake-key", Model = "test" }
        };
        cs.Identifier = cs.GenerateIdentifier();
        return cs;
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task CannotRemoveRavenConnectionStringUsedBySinkPullReplication()
    {
        using var store = GetDocumentStore();

        var cs = new RavenConnectionString { Name = "RavenCS", Database = "TargetDb", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } };
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(cs));

        await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            Name = "TestSink",
            ConnectionStringName = cs.Name,
            HubName = "TestHub"
        }));

        var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new RemoveConnectionStringOperation<RavenConnectionString>(cs)));
        Assert.Contains($"Can't delete connection string: {cs.Name}. It is used by PullReplicationAsSink 'TestSink'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.RavenConnectionStrings.ContainsKey(cs.Name));
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task CannotRemoveAiConnectionStringUsedByAiAgent()
    {
        using var store = GetDocumentStore();

        var cs = CreateAiConnectionString("AiCS");
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(cs));

        var agent = new AiAgentConfiguration("TestAgent", cs.Name, "You are a test agent.");
        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(agent, new { Answer = "answer" }));

        var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new RemoveConnectionStringOperation<AiConnectionString>(cs)));
        Assert.Contains($"Can't delete connection string: {cs.Name}. It is used by AiAgent 'TestAgent'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.AiConnectionStrings.ContainsKey(cs.Name));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task CannotRemoveSqlConnectionStringUsedByCdcSink()
    {
        using var store = GetDocumentStore();

        var cs = CreateSqlConnectionString("SqlCS");
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(cs));
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("TestCdc", cs.Name)));

        var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new RemoveConnectionStringOperation<SqlConnectionString>(cs)));
        Assert.Contains($"Can't delete connection string: {cs.Name}. It is used by CdcSink 'TestCdc'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.SqlConnectionStrings.ContainsKey(cs.Name));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task CanRemoveSqlConnectionStringAfterCdcSinkIsDeleted()
    {
        using var store = GetDocumentStore();

        var cs = CreateSqlConnectionString("SqlCS");
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(cs));
        var added = await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("TestCdc", cs.Name)));

        await store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(added.TaskId, OngoingTaskType.CdcSink));
        await store.Maintenance.SendAsync(new RemoveConnectionStringOperation<SqlConnectionString>(cs));

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.False(record.SqlConnectionStrings.ContainsKey(cs.Name));
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Sinks)]
    public async Task CannotDeleteServerWideSqlConnectionStringInUseByCdcSink()
    {
        using var store = GetDocumentStore();

        var cs = CreateSqlConnectionString("ServerWideSqlCS");
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = cs }));

        var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(cs.Name);
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("TestCdc", prefixedName)));

        var ex = await Assert.ThrowsAsync<RavenException>(() =>
            store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<SqlConnectionString>(new SqlConnectionString { Name = cs.Name })));
        Assert.Contains("It is used by CdcSink 'TestCdc'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.SqlConnectionStrings.ContainsKey(prefixedName));
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Sinks)]
    public async Task CannotExcludeDatabaseFromServerWideSqlConnectionStringInUseByCdcSink()
    {
        using var store = GetDocumentStore();

        var cs = CreateSqlConnectionString("ServerWideSqlCS");
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = cs }));

        var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(cs.Name);
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("TestCdc", prefixedName)));

        var ex = await Assert.ThrowsAsync<RavenException>(() =>
            store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = cs, ExcludedDatabases = new[] { store.Database } })));
        Assert.Contains("It is used by CdcSink 'TestCdc'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.SqlConnectionStrings.ContainsKey(prefixedName));
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Replication)]
    public async Task CannotDeleteServerWideRavenConnectionStringInUseBySinkPullReplication()
    {
        using var store = GetDocumentStore();

        var cs = new RavenConnectionString { Name = "ServerWideRavenCS", Database = "TargetDb", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } };
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = cs }));

        var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(cs.Name);
        await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            Name = "TestSink",
            ConnectionStringName = prefixedName,
            HubName = "TestHub"
        }));

        var ex = await Assert.ThrowsAsync<RavenException>(() =>
            store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<RavenConnectionString>(new RavenConnectionString { Name = cs.Name })));
        Assert.Contains("It is used by PullReplicationAsSink 'TestSink'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.RavenConnectionStrings.ContainsKey(prefixedName));
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Ai)]
    public async Task CannotDeleteServerWideAiConnectionStringInUseByAiAgent()
    {
        using var store = GetDocumentStore();

        var cs = CreateAiConnectionString("ServerWideAiCS");
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = cs }));

        var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(cs.Name);
        var agent = new AiAgentConfiguration("TestAgent", prefixedName, "You are a test agent.");
        await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(agent, new { Answer = "answer" }));

        var ex = await Assert.ThrowsAsync<RavenException>(() =>
            store.Maintenance.Server.SendAsync(new RemoveServerWideConnectionStringOperation<AiConnectionString>(new AiConnectionString { Name = cs.Name })));
        Assert.Contains("It is used by AiAgent 'TestAgent'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.True(record.AiConnectionStrings.ContainsKey(prefixedName));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public async Task ConnectionStringNamesDifferingOnlyInCaseAreDistinctConsumers()
    {
        using var store = GetDocumentStore();

        var lower = CreateSqlConnectionString("cs");
        var upper = CreateSqlConnectionString("CS");
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(lower));
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(upper));
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("TestCdc", upper.Name)));

        await store.Maintenance.SendAsync(new RemoveConnectionStringOperation<SqlConnectionString>(lower));

        var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new RemoveConnectionStringOperation<SqlConnectionString>(upper)));
        Assert.Contains($"Can't delete connection string: {upper.Name}. It is used by CdcSink 'TestCdc'", ex.Message);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        Assert.False(record.SqlConnectionStrings.ContainsKey(lower.Name));
        Assert.True(record.SqlConnectionStrings.ContainsKey(upper.Name));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sinks)]
    public async Task UsedByListsOnlyConsumersOfTheSameConnectionStringType()
    {
        using var store = GetDocumentStore();

        var ravenCs = new RavenConnectionString { Name = "Shared", Database = "TargetDb", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } };
        var sqlCs = CreateSqlConnectionString("Shared");
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(ravenCs));
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(sqlCs));

        await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication("TargetDb", ravenCs.Name) { Name = "RavenTask" }));
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("SqlTask", sqlCs.Name)));

        var result = await store.Maintenance.SendAsync(new GetConnectionStringsOperation());

        var ravenUsage = Assert.Single(result.RavenConnectionStrings[ravenCs.Name].UsedBy);
        Assert.Equal(ConnectionStringUsageKind.ExternalReplication, ravenUsage.Kind);
        Assert.Equal("RavenTask", ravenUsage.Name);

        var sqlUsage = Assert.Single(result.SqlConnectionStrings[sqlCs.Name].UsedBy);
        Assert.Equal(ConnectionStringUsageKind.CdcSink, sqlUsage.Kind);
        Assert.Equal("SqlTask", sqlUsage.Name);
    }

    [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Replication | RavenTestCategory.Sinks)]
    public async Task ServerWideUsedByListsOnlyConsumersOfTheSameConnectionStringType()
    {
        using var store = GetDocumentStore();

        var ravenCs = new RavenConnectionString { Name = "Shared", Database = "TargetDb", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } };
        var sqlCs = CreateSqlConnectionString("Shared");
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = ravenCs }));
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(new ServerWideConnectionString { ConnectionString = sqlCs }));

        var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName("Shared");
        await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication("TargetDb", prefixedName) { Name = "RavenTask" }));
        await store.Maintenance.SendAsync(new AddCdcSinkOperation(CreateCdcSink("SqlTask", prefixedName)));

        var result = await store.Maintenance.Server.SendAsync(new GetServerWideConnectionStringsOperation());

        var ravenUsage = Assert.Single(Assert.Single(result.Results, x => x.Type == ConnectionStringType.Raven).UsedBy);
        Assert.Equal(ConnectionStringUsageKind.ExternalReplication, ravenUsage.Kind);
        Assert.Equal("RavenTask", ravenUsage.Name);

        var sqlUsage = Assert.Single(Assert.Single(result.Results, x => x.Type == ConnectionStringType.Sql).UsedBy);
        Assert.Equal(ConnectionStringUsageKind.CdcSink, sqlUsage.Kind);
        Assert.Equal("SqlTask", sqlUsage.Name);
    }
}
