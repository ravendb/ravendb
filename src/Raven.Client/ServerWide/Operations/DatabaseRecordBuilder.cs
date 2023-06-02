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

internal class DatabaseRecordBuilder :
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
    public DatabaseRecord DatabaseRecord { get; } = new();

    IBackupConfigurationBuilder IBackupConfigurationBuilder.AddPeriodicBackup(PeriodicBackupConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.PeriodicBackups ??= new List<PeriodicBackupConfiguration>();
        DatabaseRecord.PeriodicBackups.Add(configuration);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddRavenConnectionString(RavenConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        DatabaseRecord.RavenConnectionStrings ??= new Dictionary<string, RavenConnectionString>();
        DatabaseRecord.RavenConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddSqlConnectionString(SqlConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        DatabaseRecord.SqlConnectionStrings ??= new Dictionary<string, SqlConnectionString>();
        DatabaseRecord.SqlConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddOlapConnectionString(OlapConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        DatabaseRecord.OlapConnectionStrings ??= new Dictionary<string, OlapConnectionString>();
        DatabaseRecord.OlapConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddElasticSearchConnectionString(ElasticSearchConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        DatabaseRecord.ElasticSearchConnectionStrings ??= new Dictionary<string, ElasticSearchConnectionString>();
        DatabaseRecord.ElasticSearchConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IConnectionStringConfigurationBuilder IConnectionStringConfigurationBuilder.AddQueueConnectionString(QueueConnectionString connectionString)
    {
        if (connectionString == null)
            throw new ArgumentNullException(nameof(connectionString));

        DatabaseRecord.QueueConnectionStrings ??= new Dictionary<string, QueueConnectionString>();
        DatabaseRecord.QueueConnectionStrings.Add(connectionString.Name, connectionString);

        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.Disabled()
    {
        HandleDisabled();
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.Encrypted()
    {
        HandleEncryption();
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithLockMode(DatabaseLockMode lockMode)
    {
        HandleLockMode(lockMode);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureDocumentsCompression(DocumentsCompressionConfiguration configuration)
    {
        HandleDocumentsCompression(configuration);

        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithSorters(params SorterDefinition[] sorterDefinitions)
    {
        HandleSorters(sorterDefinitions);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithAnalyzers(params AnalyzerDefinition[] analyzerDefinitions)
    {
        HandleAnalyzers(analyzerDefinitions);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithIndexes(params IndexDefinition[] indexDefinitions)
    {
        HandleIndexes(indexDefinitions);

        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithSettings(Dictionary<string, string> settings)
    {
        HandleSettings(settings);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithSettings(Action<Dictionary<string, string>> builder)
    {
        HandleSettings(builder);

        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureRevisions(RevisionsConfiguration configuration)
    {
        HandleRevisions(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithEtls(Action<IEtlConfigurationBuilder> builder)
    {
        HandleEtls(builder);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithBackups(Action<IBackupConfigurationBuilder> builder)
    {
        HandleBackups(builder);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithReplication(Action<IReplicationConfigurationBuilder> builder)
    {
        HandleReplication(builder);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder)
    {
        HandleConnectionStrings(builder);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureClient(ClientConfiguration configuration)
    {
        HandleClient(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureStudio(StudioConfiguration configuration)
    {
        HandleStudio(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureRefresh(RefreshConfiguration configuration)
    {
        HandleRefresh(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureExpiration(ExpirationConfiguration configuration)
    {
        HandleExpiration(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.ConfigureTimeSeries(TimeSeriesConfiguration configuration)
    {
        HandleTimeSeries(configuration);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>.WithIntegrations(Action<IIntegrationConfigurationBuilder> builder)
    {
        HandleIntegrations(builder);
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilder.WithTopology(DatabaseTopology topology)
    {
        DatabaseRecord.Topology = topology ?? throw new ArgumentNullException(nameof(topology));
        return this;
    }

    IDatabaseRecordBuilder IDatabaseRecordBuilder.WithTopology(Action<ITopologyConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
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

        DatabaseRecord.Sharding = new ShardingConfiguration();
        builder(this);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddRavenEtl(RavenEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.RavenEtls ??= new List<RavenEtlConfiguration>();
        DatabaseRecord.RavenEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddSqlEtl(SqlEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.SqlEtls ??= new List<SqlEtlConfiguration>();
        DatabaseRecord.SqlEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddElasticSearchEtl(ElasticSearchEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.ElasticSearchEtls ??= new List<ElasticSearchEtlConfiguration>();
        DatabaseRecord.ElasticSearchEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddOlapEtl(OlapEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.OlapEtls ??= new List<OlapEtlConfiguration>();
        DatabaseRecord.OlapEtls.Add(configuration);

        return this;
    }

    IEtlConfigurationBuilder IEtlConfigurationBuilder.AddQueueEtl(QueueEtlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.QueueEtls ??= new List<QueueEtlConfiguration>();
        DatabaseRecord.QueueEtls.Add(configuration);

        return this;
    }

    IIntegrationConfigurationBuilder IIntegrationConfigurationBuilder.ConfigurePostgreSql(PostgreSqlConfiguration configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.Integrations ??= new IntegrationConfigurations();
        DatabaseRecord.Integrations.PostgreSql = configuration;

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddExternalReplication(ExternalReplication configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.ExternalReplications ??= new List<ExternalReplication>();
        DatabaseRecord.ExternalReplications.Add(configuration);

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddPullReplicationSink(PullReplicationAsSink configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.SinkPullReplications ??= new List<PullReplicationAsSink>();
        DatabaseRecord.SinkPullReplications.Add(configuration);

        return this;
    }

    IReplicationConfigurationBuilder IReplicationConfigurationBuilder.AddPullReplicationHub(PullReplicationDefinition configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        DatabaseRecord.HubPullReplications ??= new List<PullReplicationDefinition>();
        DatabaseRecord.HubPullReplications.Add(configuration);

        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.Encrypted()
    {
        HandleEncryption();
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithLockMode(DatabaseLockMode lockMode)
    {
        HandleLockMode(lockMode);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureDocumentsCompression(DocumentsCompressionConfiguration configuration)
    {
        HandleDocumentsCompression(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithSorters(params SorterDefinition[] sorterDefinitions)
    {
        HandleSorters(sorterDefinitions);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithAnalyzers(params AnalyzerDefinition[] analyzerDefinitions)
    {
        HandleAnalyzers(analyzerDefinitions);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithIndexes(params IndexDefinition[] indexDefinitions)
    {
        HandleIndexes(indexDefinitions);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithSettings(Dictionary<string, string> settings)
    {
        HandleSettings(settings);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithSettings(Action<Dictionary<string, string>> builder)
    {
        HandleSettings(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureRevisions(RevisionsConfiguration configuration)
    {
        HandleRevisions(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithEtls(Action<IEtlConfigurationBuilder> builder)
    {
        HandleEtls(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithBackups(Action<IBackupConfigurationBuilder> builder)
    {
        HandleBackups(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithReplication(Action<IReplicationConfigurationBuilder> builder)
    {
        HandleReplication(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder)
    {
        HandleConnectionStrings(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureClient(ClientConfiguration configuration)
    {
        HandleClient(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureStudio(StudioConfiguration configuration)
    {
        HandleStudio(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureRefresh(RefreshConfiguration configuration)
    {
        HandleRefresh(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureExpiration(ExpirationConfiguration configuration)
    {
        HandleExpiration(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.ConfigureTimeSeries(TimeSeriesConfiguration configuration)
    {
        HandleTimeSeries(configuration);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.WithIntegrations(Action<IIntegrationConfigurationBuilder> builder)
    {
        HandleIntegrations(builder);
        return this;
    }

    IShardedDatabaseRecordBuilder IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>.Disabled()
    {
        HandleDisabled();
        return this;
    }

    ITopologyConfigurationBuilder ITopologyConfigurationBuilderBase<ITopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        DatabaseRecord.Topology ??= new DatabaseTopology();
        DatabaseRecord.Topology.Members.Add(nodeTag);

        return this;
    }

    //ITopologyConfigurationBuilder ITopologyConfigurationBuilder.EnableDynamicNodesDistribution()
    //{
    //    DatabaseRecord.Topology ??= new DatabaseTopology();
    //    DatabaseRecord.Topology.DynamicNodesDistribution = true;

    //    return this;
    //}

    private void HandleDisabled()
    {
        DatabaseRecord.Disabled = true;
    }

    private void HandleEncryption()
    {
        DatabaseRecord.Encrypted = true;
    }

    private void HandleLockMode(DatabaseLockMode lockMode)
    {
        DatabaseRecord.LockMode = lockMode;
    }

    private void HandleDocumentsCompression(DocumentsCompressionConfiguration configuration)
    {
        DatabaseRecord.DocumentsCompression = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleSorters(SorterDefinition[] sorterDefinitions)
    {
        if (sorterDefinitions == null || sorterDefinitions.Length == 0)
            return;

        DatabaseRecord.Sorters ??= new Dictionary<string, SorterDefinition>();

        foreach (SorterDefinition sorterDefinition in sorterDefinitions)
            DatabaseRecord.Sorters.Add(sorterDefinition.Name, sorterDefinition);
    }

    private void HandleAnalyzers(AnalyzerDefinition[] analyzerDefinitions)
    {
        if (analyzerDefinitions == null || analyzerDefinitions.Length == 0)
            return;

        DatabaseRecord.Analyzers ??= new Dictionary<string, AnalyzerDefinition>();

        foreach (AnalyzerDefinition analyzerDefinition in analyzerDefinitions)
            DatabaseRecord.Analyzers.Add(analyzerDefinition.Name, analyzerDefinition);
    }

    private void HandleIndexes(IndexDefinition[] indexDefinitions)
    {
        if (indexDefinitions == null || indexDefinitions.Length == 0)
            return;

        DatabaseRecord.Indexes ??= new Dictionary<string, IndexDefinition>();

        foreach (IndexDefinition indexDefinition in indexDefinitions)
            DatabaseRecord.Indexes.Add(indexDefinition.Name, indexDefinition);
    }

    private void HandleSettings(Dictionary<string, string> settings)
    {
        DatabaseRecord.Settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }

    private void HandleSettings(Action<Dictionary<string, string>> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        DatabaseRecord.Settings = new Dictionary<string, string>();
        builder(DatabaseRecord.Settings);
    }

    private void HandleRevisions(RevisionsConfiguration configuration)
    {
        DatabaseRecord.Revisions = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleEtls(Action<IEtlConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
    }

    private void HandleBackups(Action<IBackupConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
    }

    private void HandleReplication(Action<IReplicationConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
    }

    private void HandleConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
    }

    private void HandleClient(ClientConfiguration configuration)
    {
        DatabaseRecord.Client = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleStudio(StudioConfiguration configuration)
    {
        DatabaseRecord.Studio = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleRefresh(RefreshConfiguration configuration)
    {
        DatabaseRecord.Refresh = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleExpiration(ExpirationConfiguration configuration)
    {
        DatabaseRecord.Expiration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleTimeSeries(TimeSeriesConfiguration configuration)
    {
        DatabaseRecord.TimeSeries = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    private void HandleIntegrations(Action<IIntegrationConfigurationBuilder> builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        builder(this);
    }

    private void WithName(string databaseName)
    {
        ResourceNameValidator.AssertValidDatabaseName(databaseName);
        DatabaseRecord.DatabaseName = databaseName;
    }

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.Orchestrator(OrchestratorTopology topology)
    {
        if (topology == null) 
            throw new ArgumentNullException(nameof(topology));

        DatabaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
        DatabaseRecord.Sharding.Orchestrator.Topology = topology;
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

        DatabaseRecord.Sharding.Shards ??= new Dictionary<int, DatabaseTopology>();
        DatabaseRecord.Sharding.Shards.Add(shardNumber, topology);
        return this;
    }

    private DatabaseTopology _shardTopology;

    IShardedTopologyConfigurationBuilder IShardedTopologyConfigurationBuilder.AddShard(int shardNumber, Action<IShardTopologyConfigurationBuilder> builder)
    {
        if (builder == null) 
            throw new ArgumentNullException(nameof(builder));

        _shardTopology = new DatabaseTopology();
        try
        {
            builder(this);

            DatabaseRecord.Sharding.Shards ??= new Dictionary<int, DatabaseTopology>();
            DatabaseRecord.Sharding.Shards.Add(shardNumber, _shardTopology);
        }
        finally
        {
            _shardTopology = null;
        }

        return this;
    }

    IOrchestratorTopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IOrchestratorTopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        if (string.IsNullOrEmpty(nodeTag)) 
            throw new ArgumentException("Value cannot be null or empty.", nameof(nodeTag));

        DatabaseRecord.Sharding.Orchestrator ??= new OrchestratorConfiguration();
        DatabaseRecord.Sharding.Orchestrator.Topology ??= new OrchestratorTopology();
        DatabaseRecord.Sharding.Orchestrator.Topology.Members.Add(nodeTag);

        return this;
    }

    IShardTopologyConfigurationBuilder ITopologyConfigurationBuilderBase<IShardTopologyConfigurationBuilder>.AddNode(string nodeTag)
    {
        if (string.IsNullOrEmpty(nodeTag)) 
            throw new ArgumentException("Value cannot be null or empty.", nameof(nodeTag));

        _shardTopology.Members.Add(nodeTag);
        return this;
    }
}

public interface IDatabaseRecordBuilderInitializer
{
    public IDatabaseRecordBuilder Regular(string databaseName);

    public IShardedDatabaseRecordBuilder Sharded(string databaseName, Action<IShardedTopologyConfigurationBuilder> builder);
}

public interface IDatabaseRecordBuilder : IDatabaseRecordBuilderBase<IDatabaseRecordBuilder>
{
    public IDatabaseRecordBuilder WithTopology(DatabaseTopology topology);

    public IDatabaseRecordBuilder WithTopology(Action<ITopologyConfigurationBuilder> builder);
}

public interface IShardedDatabaseRecordBuilder : IDatabaseRecordBuilderBase<IShardedDatabaseRecordBuilder>
{
}

public interface IDatabaseRecordBuilderBase<out TSelf>
{
    TSelf Disabled();

    TSelf Encrypted();

    TSelf WithLockMode(DatabaseLockMode lockMode);

    TSelf ConfigureDocumentsCompression(DocumentsCompressionConfiguration configuration);

    TSelf WithSorters(params SorterDefinition[] sorterDefinitions);

    TSelf WithAnalyzers(params AnalyzerDefinition[] analyzerDefinitions);

    TSelf WithIndexes(params IndexDefinition[] indexDefinitions);

    TSelf WithSettings(Dictionary<string, string> settings);

    TSelf WithSettings(Action<Dictionary<string, string>> builder);

    TSelf ConfigureRevisions(RevisionsConfiguration configuration);

    TSelf WithEtls(Action<IEtlConfigurationBuilder> builder);

    TSelf WithBackups(Action<IBackupConfigurationBuilder> builder);

    TSelf WithReplication(Action<IReplicationConfigurationBuilder> builder);

    TSelf WithConnectionStrings(Action<IConnectionStringConfigurationBuilder> builder);

    TSelf ConfigureClient(ClientConfiguration configuration);

    TSelf ConfigureStudio(StudioConfiguration configuration);

    TSelf ConfigureRefresh(RefreshConfiguration configuration);

    TSelf ConfigureExpiration(ExpirationConfiguration configuration);

    TSelf ConfigureTimeSeries(TimeSeriesConfiguration configuration);

    TSelf WithIntegrations(Action<IIntegrationConfigurationBuilder> builder);
}

public interface ITopologyConfigurationBuilderBase<out TSelf>
{
    TSelf AddNode(string nodeTag);
}

public interface ITopologyConfigurationBuilder : ITopologyConfigurationBuilderBase<ITopologyConfigurationBuilder>
{
    //ITopologyConfigurationBuilder EnableDynamicNodesDistribution();
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
