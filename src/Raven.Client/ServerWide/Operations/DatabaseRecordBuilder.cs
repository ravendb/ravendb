using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;

namespace Raven.Client.ServerWide.Operations;

public sealed class DatabaseRecordBuilder :
    IDatabaseRecordBuilderInitializer,
    IDatabaseRecordBuilder,
    IEtlConfigurationBuilder,
    IConnectionStringConfigurationBuilder,
    IBackupConfigurationBuilder,
    IIntegrationConfigurationBuilder,
    IReplicationConfigurationBuilder,
    ITopologyConfigurationBuilder,
    IShardedDatabaseRecordBuilder,
    IShardedTopologyConfigurationBuilder,
    IOrchestratorTopologyConfigurationBuilder,
    IShardTopologyConfigurationBuilder
{
    public static IDatabaseRecordBuilderInitializer Create() => new DatabaseRecordBuilder();

    private DatabaseRecordBuilder()
    {
        _databaseRecord = new DatabaseRecord();
    }

    private DatabaseTopology _shardTopology;
    private readonly DatabaseRecord _databaseRecord;

    IBackupConfigurationBuilder IBackupConfigurationBuilder.AddPeriodicBackup(PeriodicBackupConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.PeriodicBackups ??= new List<PeriodicBackupConfiguration>();
        _databaseRecord.PeriodicBackups.Add(configuration);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddRavenConnectionString(RavenConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        _databaseRecord.RavenConnectionStrings ??= new Dictionary<string, RavenConnectionString>();
        _databaseRecord.RavenConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddSqlConnectionString(SqlConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        _databaseRecord.SqlConnectionStrings ??= new Dictionary<string, SqlConnectionString>();
        _databaseRecord.SqlConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddOlapConnectionString(OlapConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        _databaseRecord.OlapConnectionStrings ??= new Dictionary<string, OlapConnectionString>();
        _databaseRecord.OlapConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddElasticSearchConnectionString(ElasticSearchConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        _databaseRecord.ElasticSearchConnectionStrings ??= new Dictionary<string, ElasticSearchConnectionString>();
        _databaseRecord.ElasticSearchConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddQueueConnectionString(QueueConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        _databaseRecord.QueueConnectionStrings ??= new Dictionary<string, QueueConnectionString>();
        _databaseRecord.QueueConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderInitializer.Regular(string databaseName)
    {
        WithName(databaseName);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderInitializer.Sharded(string databaseName, Action<IShardedTopologyConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        WithName(databaseName);

        _databaseRecord.Sharding = new ShardingConfiguration();
        builder(this);

        if (_databaseRecord.Sharding.Shards == null || _databaseRecord.Sharding.Shards.Count == 0)
            throw new InvalidOperationException($"At least one shard is required. Use '{nameof(IShardedTopologyConfigurationBuilder.AddShard)}' to add a shard to the topology.");

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddRavenEtl(RavenEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.RavenEtls ??= new List<RavenEtlConfiguration>();
        _databaseRecord.RavenEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddSqlEtl(SqlEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.SqlEtls ??= new List<SqlEtlConfiguration>();
        _databaseRecord.SqlEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddElasticSearchEtl(ElasticSearchEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.ElasticSearchEtls ??= new List<ElasticSearchEtlConfiguration>();
        _databaseRecord.ElasticSearchEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddOlapEtl(OlapEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.OlapEtls ??= new List<OlapEtlConfiguration>();
        _databaseRecord.OlapEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddQueueEtl(QueueEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.QueueEtls ??= new List<QueueEtlConfiguration>();
        _databaseRecord.QueueEtls.Add(configuration);

        return this;
    }

    IIntegrationConfigurationBuilder IIntegrationConfigurationBuilder.ConfigurePostgreSql(PostgreSqlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.Integrations ??= new IntegrationConfigurations();
        _databaseRecord.Integrations.PostgreSql = configuration;

        return this;
    }

    IOrchestratorTopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IOrchestratorTopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        if (string.IsNullOrEmpty(nodeTag))
            throw new ArgumentException("Value cannot be null or empty.", nameof(nodeTag));

        _databaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
        _databaseRecord.Sharding.Orchestrator.Topology ??= new OrchestratorTopology();
        _databaseRecord.Sharding.Orchestrator.Topology.Members.Add(nodeTag);

        return this;
    }

    ITopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IShardTopologyConfigurationBuilder>.EnableDynamicNodesDistribution()
    {
        _shardTopology.DynamicNodesDistribution = true;
        return this;
    }

    ITopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IOrchestratorTopologyConfigurationBuilder>.EnableDynamicNodesDistribution()
    {
        _databaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
        _databaseRecord.Sharding.Orchestrator.Topology ??= new OrchestratorTopology();
        _databaseRecord.Sharding.Orchestrator.Topology.DynamicNodesDistribution = true;

        return this;
    }

    ITopologyConfigurationBuilder ITopologyConfigurationBuilderBase<ITopologyConfigurationBuilder>.EnableDynamicNodesDistribution()
    {
        _databaseRecord.Topology ??= new DatabaseTopology();
        _databaseRecord.Topology.DynamicNodesDistribution = true;

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddExternalReplication(ExternalReplication configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.ExternalReplications ??= new List<ExternalReplication>();
        _databaseRecord.ExternalReplications.Add(configuration);

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddPullReplicationSink(PullReplicationAsSink configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.SinkPullReplications ??= new List<PullReplicationAsSink>();
        _databaseRecord.SinkPullReplications.Add(configuration);

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddPullReplicationHub(PullReplicationDefinition configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        _databaseRecord.HubPullReplications ??= new List<PullReplicationDefinition>();
        _databaseRecord.HubPullReplications.Add(configuration);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.Encrypted()
    {
        _databaseRecord.Encrypted = true;
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithLockMode(DatabaseLockMode lockMode)
    {
        _databaseRecord.LockMode = lockMode;
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureDocumentsCompression(DocumentsCompressionConfiguration configuration)
    {
        _databaseRecord.DocumentsCompression = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithSorters(params SorterDefinition[] sorterDefinitions)
    {
        if (sorterDefinitions == null || sorterDefinitions.Length == 0)
            return this;

        _databaseRecord.Sorters ??= new Dictionary<string, SorterDefinition>();

        foreach (SorterDefinition sorterDefinition in sorterDefinitions)
            _databaseRecord.Sorters.Add(sorterDefinition.Name, sorterDefinition);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithAnalyzers(params AnalyzerDefinition[] analyzerDefinitions)
    {
        if (analyzerDefinitions == null || analyzerDefinitions.Length == 0)
            return this;

        _databaseRecord.Analyzers ??= new Dictionary<string, AnalyzerDefinition>();

        foreach (AnalyzerDefinition analyzerDefinition in analyzerDefinitions)
            _databaseRecord.Analyzers.Add(analyzerDefinition.Name, analyzerDefinition);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithIndexes(params IndexDefinition[] indexDefinitions)
    {
        if (indexDefinitions == null || indexDefinitions.Length == 0)
            return this;

        _databaseRecord.Indexes ??= new Dictionary<string, IndexDefinition>();

        foreach (IndexDefinition indexDefinition in indexDefinitions)
            _databaseRecord.Indexes.Add(indexDefinition.Name, indexDefinition);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithSettings(Dictionary<string, string> settings)
    {
        _databaseRecord.Settings = settings ?? throw new ArgumentNullException(nameof(settings));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithSettings(Action<Dictionary<string, string>> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        _databaseRecord.Settings = new Dictionary<string, string>();
        builder(_databaseRecord.Settings);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureRevisions(RevisionsConfiguration configuration)
    {
        _databaseRecord.Revisions = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }
    
    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureRevisionsBin(RevisionsBinConfiguration configuration)
    {
        _databaseRecord.RevisionsBin = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithEtls(Action<IEtlConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithBackups(Action<IBackupConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithReplication(Action<IReplicationConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureClient(ClientConfiguration configuration)
    {
        _databaseRecord.Client = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureStudio(StudioConfiguration configuration)
    {
        _databaseRecord.Studio = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureRefresh(RefreshConfiguration configuration)
    {
        _databaseRecord.Refresh = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureExpiration(ExpirationConfiguration configuration)
    {
        _databaseRecord.Expiration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.ConfigureTimeSeries(TimeSeriesConfiguration configuration)
    {
        _databaseRecord.TimeSeries = configuration ?? throw new ArgumentNullException(nameof(configuration));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.WithIntegrations(Action<IIntegrationConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);

        return this;
    }

    DatabaseRecord IDatabaseRecordBuilderInitializer.ToDatabaseRecord()
    {
        return _databaseRecord;
    }

    DatabaseRecord IDatabaseRecordBuilderBase.ToDatabaseRecord()
    {
        return _databaseRecord;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilderBase.Disabled()
    {
        _databaseRecord.Disabled = true;
        return this;
    }

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.Orchestrator(OrchestratorTopology topology)
    {
        if (topology == null)
            throw new ArgumentNullException(nameof(topology));

        _databaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
        _databaseRecord.Sharding.Orchestrator.Topology = topology;
        return this;
    }

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.Orchestrator(Action<IOrchestratorTopologyConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
        return this;
    }

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.AddShard(int shardNumber, DatabaseTopology topology)
    {
        if (topology == null)
            throw new ArgumentNullException(nameof(topology));

        _databaseRecord.Sharding.Shards ??= new Dictionary<int, DatabaseTopology>();
        _databaseRecord.Sharding.Shards.Add(shardNumber, topology);
        return this;
    }

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.AddShard(int shardNumber, Action<IShardTopologyConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        _shardTopology = new DatabaseTopology();
        try
        {
            builder(this);

            _databaseRecord.Sharding.Shards ??= new Dictionary<int, DatabaseTopology>();
            _databaseRecord.Sharding.Shards.Add(shardNumber, _shardTopology);
        }
        finally
        {
            _shardTopology = null;
        }

        return this;
    }

    IShardTopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IShardTopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        if (string.IsNullOrEmpty(nodeTag))
            throw new ArgumentException("Value cannot be null or empty.", nameof(nodeTag));

        _shardTopology.Members.Add(nodeTag);
        return this;
    }

    ITopologyConfigurationBuilder ITopologyConfigurationBuilderBase<ITopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        _databaseRecord.Topology ??= new DatabaseTopology();
        _databaseRecord.Topology.Members.Add(nodeTag);

        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilder.WithTopology(DatabaseTopology topology)
    {
        _databaseRecord.Topology = topology ?? throw new ArgumentNullException(nameof(topology));
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilder.WithTopology(Action<ITopologyConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
        return this;
    }

    IDatabaseRecordBuilderBase IDatabaseRecordBuilder.WithReplicationFactor(int replicationFactor)
    {
        _databaseRecord.Topology ??= new DatabaseTopology();
        _databaseRecord.Topology.ReplicationFactor = replicationFactor;

        return this;
    }

    private void WithName(string databaseName)
    {
        ResourceNameValidator.AssertValidDatabaseName(databaseName);
        _databaseRecord.DatabaseName = databaseName;
    }
}

public interface IDatabaseRecordBuilderInitializer
{
    public IDatabaseRecordBuilder Regular(string databaseName);

    public IShardedDatabaseRecordBuilder Sharded(string databaseName, Action<IShardedTopologyConfigurationBuilder> builder);

    public DatabaseRecord ToDatabaseRecord();
}

public interface IDatabaseRecordBuilder : IDatabaseRecordBuilderBase
{
    public IDatabaseRecordBuilderBase WithTopology(DatabaseTopology topology);

    public IDatabaseRecordBuilderBase WithTopology(Action<ITopologyConfigurationBuilder> builder);

    public IDatabaseRecordBuilderBase WithReplicationFactor(int replicationFactor);
}

public interface IShardedDatabaseRecordBuilder : IDatabaseRecordBuilderBase
{
}

public interface IDatabaseRecordBuilderBase
{
    DatabaseRecord ToDatabaseRecord();

    IDatabaseRecordBuilderBase Disabled();

    IDatabaseRecordBuilderBase Encrypted();

    IDatabaseRecordBuilderBase WithLockMode(DatabaseLockMode lockMode);

    IDatabaseRecordBuilderBase ConfigureDocumentsCompression(DocumentsCompressionConfiguration configuration);

    IDatabaseRecordBuilderBase WithSorters(params SorterDefinition[] sorterDefinitions);

    IDatabaseRecordBuilderBase WithAnalyzers(params AnalyzerDefinition[] analyzerDefinitions);

    IDatabaseRecordBuilderBase WithIndexes(params IndexDefinition[] indexDefinitions);

    IDatabaseRecordBuilderBase WithSettings(Dictionary<string, string> settings);

    IDatabaseRecordBuilderBase WithSettings(Action<Dictionary<string, string>> builder);

    IDatabaseRecordBuilderBase ConfigureRevisions(RevisionsConfiguration configuration);

    IDatabaseRecordBuilderBase ConfigureRevisionsBin(RevisionsBinConfiguration configuration);

    IDatabaseRecordBuilderBase WithEtls(Action<IEtlConfigurationBuilder> builder);

    IDatabaseRecordBuilderBase WithBackups(Action<IBackupConfigurationBuilder> builder);

    IDatabaseRecordBuilderBase WithReplication(Action<IReplicationConfigurationBuilder> builder);

    IDatabaseRecordBuilderBase WithConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder);

    IDatabaseRecordBuilderBase ConfigureClient(ClientConfiguration configuration);

    IDatabaseRecordBuilderBase ConfigureStudio(StudioConfiguration configuration);

    IDatabaseRecordBuilderBase ConfigureRefresh(RefreshConfiguration configuration);

    IDatabaseRecordBuilderBase ConfigureExpiration(ExpirationConfiguration configuration);

    IDatabaseRecordBuilderBase ConfigureTimeSeries(TimeSeriesConfiguration configuration);

    IDatabaseRecordBuilderBase WithIntegrations(Action<IIntegrationConfigurationBuilder> builder);
}

public interface ITopologyConfigurationBuilderBase<out TSelf>
{
    TSelf AddNode(string nodeTag);

    ITopologyConfigurationBuilder EnableDynamicNodesDistribution();
}

public interface ITopologyConfigurationBuilder : ITopologyConfigurationBuilderBase<ITopologyConfigurationBuilder>
{
}

public interface IOrchestratorTopologyConfigurationBuilder : ITopologyConfigurationBuilderBase<IOrchestratorTopologyConfigurationBuilder>
{
}

public interface IShardTopologyConfigurationBuilder : ITopologyConfigurationBuilderBase<IShardTopologyConfigurationBuilder>
{
}

public interface IShardedTopologyConfigurationBuilder
{
    IShardedTopologyConfigurationBuilder Orchestrator(OrchestratorTopology topology);

    IShardedTopologyConfigurationBuilder Orchestrator(Action<IOrchestratorTopologyConfigurationBuilder> builder);

    IShardedTopologyConfigurationBuilder AddShard(int shardNumber, DatabaseTopology topology);

    IShardedTopologyConfigurationBuilder AddShard(int shardNumber, Action<IShardTopologyConfigurationBuilder> builder);
}

public interface IIntegrationConfigurationBuilder
{
    IIntegrationConfigurationBuilder ConfigurePostgreSql(PostgreSqlConfiguration configuration);
}

public interface IConnectionStringConfigurationBuilder
{
    IConnectionStringConfigurationBuilder AddRavenConnectionString(RavenConnectionString connectionString);

    IConnectionStringConfigurationBuilder AddSqlConnectionString(SqlConnectionString connectionString);

    IConnectionStringConfigurationBuilder AddOlapConnectionString(OlapConnectionString connectionString);

    IConnectionStringConfigurationBuilder AddElasticSearchConnectionString(ElasticSearchConnectionString connectionString);

    IConnectionStringConfigurationBuilder AddQueueConnectionString(QueueConnectionString connectionString);
}

public interface IReplicationConfigurationBuilder
{
    public IReplicationConfigurationBuilder AddExternalReplication(ExternalReplication configuration);

    public IReplicationConfigurationBuilder AddPullReplicationSink(PullReplicationAsSink configuration);

    public IReplicationConfigurationBuilder AddPullReplicationHub(PullReplicationDefinition configuration);
}

public interface IBackupConfigurationBuilder
{
    IBackupConfigurationBuilder AddPeriodicBackup(PeriodicBackupConfiguration configuration);
}

public interface IEtlConfigurationBuilder
{
    IEtlConfigurationBuilder AddRavenEtl(RavenEtlConfiguration configuration);

    IEtlConfigurationBuilder AddSqlEtl(SqlEtlConfiguration configuration);

    IEtlConfigurationBuilder AddElasticSearchEtl(ElasticSearchEtlConfiguration configuration);

    IEtlConfigurationBuilder AddOlapEtl(OlapEtlConfiguration configuration);

    IEtlConfigurationBuilder AddQueueEtl(QueueEtlConfiguration configuration);
}
