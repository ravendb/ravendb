using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_19938 : RavenTestBase
{
    public RavenDB_19938(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Create_Database_Via_Builder()
    {
        using (var store = GetDocumentStore(new Options
        {
            CreateDatabase = false
        }))
        {
            store.Maintenance.Server.Send(new CreateDatabaseOperation(builder => builder.Regular(store.Database)));

            var database = await GetDatabase(store.Database);
            Assert.NotNull(database);
        }
    }

    [Fact]
    public void Regular()
    {
        var record = CreateDatabaseRecord(builder => builder.Regular("DB1"));
        Assert.Equal("DB1", record.DatabaseName);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithTopology(new DatabaseTopology { Members = { "A" } })
        );

        Assert.Equal(new[] { "A" }, record.Topology.Members);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithTopology(topology => topology
                .AddNode("B")
                .AddNode("C"))
        );

        Assert.Equal(new[] { "B", "C" }, record.Topology.Members);
    }

    [Fact]
    public void Sharded()
    {
        var record = CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology
                .AddShard(0, shard => shard.AddNode("A"))
                .AddShard(1, new DatabaseTopology { Members = new List<string> { "B", "C" } })
                .AddShard(2, shard => shard.AddNode("C").AddNode("A"))
            ));

        Assert.Equal(new[] { "A" }, record.Sharding.Shards[0].Members);
        Assert.Equal(new[] { "B", "C" }, record.Sharding.Shards[1].Members);
        Assert.Equal(new[] { "C", "A" }, record.Sharding.Shards[2].Members);

        record = CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology.Orchestrator(new OrchestratorTopology { Members = new List<string> { "A" } })
        ));

        Assert.Equal(new[] { "A" }, record.Sharding.Orchestrator.Topology.Members);

        record = CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology.Orchestrator(orchestrator => orchestrator.AddNode("B").AddNode("C"))
        ));

        Assert.Equal(new[] { "B", "C" }, record.Sharding.Orchestrator.Topology.Members);
    }

    [Fact]
    public void Common()
    {
        var record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .Disabled()
        );

        Assert.True(record.Disabled);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .Encrypted()
        );

        Assert.True(record.Encrypted);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithAnalyzers(new AnalyzerDefinition { Name = "A1" })
        );

        Assert.True(record.Analyzers.ContainsKey("A1"));

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithIndexes(new IndexDefinition { Name = "I1" })
        );

        Assert.True(record.Indexes.ContainsKey("I1"));

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithSorters(new SorterDefinition { Name = "S1" })
        );

        Assert.True(record.Sorters.ContainsKey("S1"));
    }

    private static DatabaseRecord CreateDatabaseRecord(Action<IDatabaseRecordBuilderInitializer> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        var instance = new DatabaseRecordBuilder();
        builder(instance);

        return instance.DatabaseRecord;
    }
}
