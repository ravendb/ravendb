using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
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

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithReplicationFactor(3)
        );

        Assert.Empty(record.Topology.Members);
        Assert.Equal(3, record.Topology.ReplicationFactor);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .Disabled()
        );

        Assert.True(record.Disabled);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureClient(new ClientConfiguration { IdentityPartsSeparator = 'z' })
        );

        Assert.Equal('z', record.Client.IdentityPartsSeparator);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureDocumentsCompression(new DocumentsCompressionConfiguration { Collections = new[] { "Orders" } })
        );

        Assert.Equal(new[] { "Orders" }, record.DocumentsCompression.Collections);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureExpiration(new ExpirationConfiguration { DeleteFrequencyInSec = 777 })
        );

        Assert.Equal(777, record.Expiration.DeleteFrequencyInSec);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureRefresh(new RefreshConfiguration { RefreshFrequencyInSec = 333 })
        );

        Assert.Equal(333, record.Refresh.RefreshFrequencyInSec);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureRevisions(new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = true } })
        );

        Assert.True(record.Revisions.Default.Disabled);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureRevisionsBin(new RevisionsBinConfiguration { MinimumEntriesAgeToKeep = TimeSpan.FromSeconds(5)})
        );

        Assert.Equal(TimeSpan.FromSeconds(5), record.RevisionsBin.MinimumEntriesAgeToKeep);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureStudio(new StudioConfiguration { Environment = StudioConfiguration.StudioEnvironment.Production })
        );

        Assert.Equal(StudioConfiguration.StudioEnvironment.Production, record.Studio.Environment);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .ConfigureTimeSeries(new TimeSeriesConfiguration { PolicyCheckFrequency = TimeSpan.FromSeconds(555) })
        );

        Assert.Equal(TimeSpan.FromSeconds(555), record.TimeSeries.PolicyCheckFrequency);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithAnalyzers(new AnalyzerDefinition { Name = "A1" })
            .WithAnalyzers(new AnalyzerDefinition { Name = "A2" })
        );

        Assert.Equal(2, record.Analyzers.Count);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithSorters(new SorterDefinition { Name = "S1" })
            .WithSorters(new SorterDefinition { Name = "A2" })
        );

        Assert.Equal(2, record.Sorters.Count);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .Encrypted()
        );

        Assert.True(record.Encrypted);

        record = CreateDatabaseRecord(builder => builder
            .Regular("DB1")
            .WithBackups(b => b.AddPeriodicBackup(new PeriodicBackupConfiguration
            {
                Disabled = true
            }))
        );

        Assert.Equal(1, record.PeriodicBackups.Count);
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

        var e = Assert.Throws<InvalidOperationException>(() => CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology.Orchestrator(new OrchestratorTopology { Members = new List<string> { "A" } })
        )));

        Assert.Equal("At least one shard is required. Use 'AddShard' to add a shard to the topology.", e.Message);

        record = CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology
                .Orchestrator(new OrchestratorTopology { Members = new List<string> { "A" } })
                .AddShard(1, new DatabaseTopology())
        ));

        Assert.Equal(new[] { "A" }, record.Sharding.Orchestrator.Topology.Members);

        record = CreateDatabaseRecord(builder => builder.Sharded("DB1", topology =>
            topology
                .Orchestrator(orchestrator => orchestrator.AddNode("B").AddNode("C"))
                .AddShard(1, new DatabaseTopology())
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

        var instance = DatabaseRecordBuilder.Create();
        builder(instance);

        return instance.ToDatabaseRecord();
    }
}
