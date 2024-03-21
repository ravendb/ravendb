using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Integrations.PostgreSQL.Commands;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    internal sealed class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, DetailedReplicationHubAccess> DetailedReplicationHubAccess = GenerateJsonDeserializationRoutine<DetailedReplicationHubAccess>();

        public static readonly Func<BlittableJsonReaderObject, UpdateClusterIdentityCommand> UpdateClusterIdentityCommand = GenerateJsonDeserializationRoutine<UpdateClusterIdentityCommand>();

        public static readonly Func<BlittableJsonReaderObject, IncrementClusterIdentityCommand> IncrementClusterIdentityCommand = GenerateJsonDeserializationRoutine<IncrementClusterIdentityCommand>();

        public static readonly Func<BlittableJsonReaderObject, IncrementClusterIdentitiesBatchCommand> IncrementClusterIdentitiesBatchCommand = GenerateJsonDeserializationRoutine<IncrementClusterIdentitiesBatchCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteMultipleValuesCommand> DeleteMultipleValuesCommand = GenerateJsonDeserializationRoutine<DeleteMultipleValuesCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseTopology> DatabaseTopology = GenerateJsonDeserializationRoutine<DatabaseTopology>();

        public static readonly Func<BlittableJsonReaderObject, ShardBucketRange> ShardRangeAssignment = GenerateJsonDeserializationRoutine<ShardBucketRange>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromDatabaseCommand> RemoveNodeFromDatabaseCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromClusterCommand> RemoveNodeFromClusterCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RefreshConfiguration> RefreshConfiguration = GenerateJsonDeserializationRoutine<RefreshConfiguration>();
        
        public static readonly Func<BlittableJsonReaderObject, DataArchivalConfiguration> DataArchivalConfiguration = GenerateJsonDeserializationRoutine<DataArchivalConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupConfiguration> PeriodicBackupConfiguration = GenerateJsonDeserializationRoutine<PeriodicBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ServerWideBackupConfiguration> ServerWideBackupConfiguration = GenerateJsonDeserializationRoutine<ServerWideBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PrefixedShardingSetting> PrefixedShardingSetting = GenerateJsonDeserializationRoutine<PrefixedShardingSetting>();

        public static readonly Func<BlittableJsonReaderObject, ServerWideExternalReplication> ServerWideExternalReplication = GenerateJsonDeserializationRoutine<ServerWideExternalReplication>();

        public static readonly Func<BlittableJsonReaderObject, ExternalReplicationState> ExternalReplicationState = GenerateJsonDeserializationRoutine<ExternalReplicationState>();

        public static readonly Func<BlittableJsonReaderObject, ShardedExternalReplicationState> ShardedExternalReplicationState = GenerateJsonDeserializationRoutine<ShardedExternalReplicationState>();

        public static readonly Func<BlittableJsonReaderObject, RestoreBackupConfiguration> RestoreBackupConfiguration = GenerateJsonDeserializationRoutine<RestoreBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RestoreFromS3Configuration> RestoreS3BackupConfiguration = GenerateJsonDeserializationRoutine<RestoreFromS3Configuration>();

        public static readonly Func<BlittableJsonReaderObject, RestoreFromAzureConfiguration> RestoreAzureBackupConfiguration = GenerateJsonDeserializationRoutine<RestoreFromAzureConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RestoreFromGoogleCloudConfiguration> RestoreGoogleCloudBackupConfiguration = GenerateJsonDeserializationRoutine<RestoreFromGoogleCloudConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, TimeSeriesConfiguration> TimeSeriesConfiguration = GenerateJsonDeserializationRoutine<TimeSeriesConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, TimeSeriesCollectionConfiguration> TimeSeriesCollectionConfiguration = GenerateJsonDeserializationRoutine<TimeSeriesCollectionConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, TimeSeriesPolicy> TimeSeriesPolicy = GenerateJsonDeserializationRoutine<TimeSeriesPolicy>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsConfiguration> RevisionsConfiguration = GenerateJsonDeserializationRoutine<RevisionsConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, DocumentsCompressionConfiguration> DocumentsCompressionConfiguration = GenerateJsonDeserializationRoutine<DocumentsCompressionConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsCollectionConfiguration> RevisionsCollectionConfiguration = GenerateJsonDeserializationRoutine<RevisionsCollectionConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PullReplicationAsSink> PullReplicationAsSink = GenerateJsonDeserializationRoutine<PullReplicationAsSink>();

        public static readonly Func<BlittableJsonReaderObject, ExternalReplication> ExternalReplication = GenerateJsonDeserializationRoutine<ExternalReplication>();

        public static readonly Func<BlittableJsonReaderObject, ConflictSolver> ConflictSolverConfig = GenerateJsonDeserializationRoutine<ConflictSolver>();

        public static readonly Func<BlittableJsonReaderObject, RavenEtlConfiguration> RavenEtlConfiguration = GenerateJsonDeserializationRoutine<RavenEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, SqlEtlConfiguration> SqlEtlConfiguration = GenerateJsonDeserializationRoutine<SqlEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, OlapEtlConfiguration> OlapEtlConfiguration = GenerateJsonDeserializationRoutine<OlapEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ElasticSearchEtlConfiguration> ElasticSearchEtlConfiguration = GenerateJsonDeserializationRoutine<ElasticSearchEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, QueueEtlConfiguration> QueueEtlConfiguration = GenerateJsonDeserializationRoutine<QueueEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ServerStore.PutRaftCommandResult> PutRaftCommandResult = GenerateJsonDeserializationRoutine<ServerStore.PutRaftCommandResult>();

        public static readonly Func<BlittableJsonReaderObject, AddOrUpdateCompareExchangeCommand.CompareExchangeResult> CompareExchangeResult = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeCommand.CompareExchangeResult>();

        public static readonly Func<BlittableJsonReaderObject, AdminJsScript> AdminJsScript = GenerateJsonDeserializationRoutine<AdminJsScript>();

        public static readonly Func<BlittableJsonReaderObject, RavenConnectionString> RavenConnectionString = GenerateJsonDeserializationRoutine<RavenConnectionString>();

        public static readonly Func<BlittableJsonReaderObject, SqlConnectionString> SqlConnectionString = GenerateJsonDeserializationRoutine<SqlConnectionString>();

        public static readonly Func<BlittableJsonReaderObject, OlapConnectionString> OlapConnectionString = GenerateJsonDeserializationRoutine<OlapConnectionString>();
        public static readonly Func<BlittableJsonReaderObject, ElasticSearchConnectionString> ElasticSearchConnectionString = GenerateJsonDeserializationRoutine<ElasticSearchConnectionString>();
        public static readonly Func<BlittableJsonReaderObject, QueueConnectionString> QueueConnectionString = GenerateJsonDeserializationRoutine<QueueConnectionString>();
        
        public static readonly Func<BlittableJsonReaderObject, QueueSinkConfiguration> QueueSinkConfiguration = GenerateJsonDeserializationRoutine<QueueSinkConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, InstallUpdatedServerCertificateCommand> InstallUpdatedServerCertificateCommand = GenerateJsonDeserializationRoutine<InstallUpdatedServerCertificateCommand>();
        public static readonly Func<BlittableJsonReaderObject, ConfirmReceiptServerCertificateCommand> ConfirmReceiptServerCertificateCommand = GenerateJsonDeserializationRoutine<ConfirmReceiptServerCertificateCommand>();
        public static readonly Func<BlittableJsonReaderObject, RecheckStatusOfServerCertificateCommand> RecheckStatusOfServerCertificateCommand = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateCommand>();
        public static readonly Func<BlittableJsonReaderObject, ConfirmServerCertificateReplacedCommand> ConfirmServerCertificateReplacedCommand = GenerateJsonDeserializationRoutine<ConfirmServerCertificateReplacedCommand>();
        public static readonly Func<BlittableJsonReaderObject, RecheckStatusOfServerCertificateReplacementCommand> RecheckStatusOfServerCertificateReplacementCommand = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateReplacementCommand>();

        public static readonly Func<BlittableJsonReaderObject, IndexHistoryEntry> IndexHistoryEntry = GenerateJsonDeserializationRoutine<IndexHistoryEntry>();
        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();
        public static readonly Func<BlittableJsonReaderObject, AutoIndexDefinition> AutoIndexDefinition = GenerateJsonDeserializationRoutine<AutoIndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, RollingIndex> RollingIndexes = GenerateJsonDeserializationRoutine<RollingIndex>();
        public static readonly Func<BlittableJsonReaderObject, SorterDefinition> SorterDefinition = GenerateJsonDeserializationRoutine<SorterDefinition>();

        public static readonly Func<BlittableJsonReaderObject, PostgreSqlConfiguration> PostgreSqlConfiguration = GenerateJsonDeserializationRoutine<PostgreSqlConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, ShardBucketMigration> BucketMigration = GenerateJsonDeserializationRoutine<ShardBucketMigration>();

        public static readonly Func<BlittableJsonReaderObject, OrchestratorConfiguration> OrchestratorConfiguration = GenerateJsonDeserializationRoutine<OrchestratorConfiguration>();
        public static readonly Func<BlittableJsonReaderObject, ShardingConfiguration> ShardingConfiguration = GenerateJsonDeserializationRoutine<ShardingConfiguration>();

        public static readonly Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>> Commands = new Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>>
        {
            [nameof(UnregisterReplicationHubAccessCommand)] = GenerateJsonDeserializationRoutine<UnregisterReplicationHubAccessCommand>(),
            [nameof(RegisterReplicationHubAccessCommand)] = GenerateJsonDeserializationRoutine<RegisterReplicationHubAccessCommand>(),
            [nameof(BulkRegisterReplicationHubAccessCommand)] = GenerateJsonDeserializationRoutine<BulkRegisterReplicationHubAccessCommand>(),
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeBatchCommand>(),
            [nameof(CleanCompareExchangeTombstonesCommand)] = GenerateJsonDeserializationRoutine<CleanCompareExchangeTombstonesCommand>(),
            [nameof(DeleteExpiredCompareExchangeCommand)] = GenerateJsonDeserializationRoutine<DeleteExpiredCompareExchangeCommand>(),
            [nameof(PutSubscriptionBatchCommand)] = GenerateJsonDeserializationRoutine<PutSubscriptionBatchCommand>(),
            [nameof(PutShardedSubscriptionBatchCommand)] = GenerateJsonDeserializationRoutine<PutShardedSubscriptionBatchCommand>(),
            [nameof(CleanUpClusterStateCommand)] = GenerateJsonDeserializationRoutine<CleanUpClusterStateCommand>(),
            [nameof(ClusterTransactionCommand)] = GenerateJsonDeserializationRoutine<ClusterTransactionCommand>(),
            [nameof(InstallUpdatedServerCertificateCommand)] = GenerateJsonDeserializationRoutine<InstallUpdatedServerCertificateCommand>(),
            [nameof(ConfirmReceiptServerCertificateCommand)] = GenerateJsonDeserializationRoutine<ConfirmReceiptServerCertificateCommand>(),
            [nameof(RecheckStatusOfServerCertificateCommand)] = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateCommand>(),
            [nameof(ConfirmServerCertificateReplacedCommand)] = GenerateJsonDeserializationRoutine<ConfirmServerCertificateReplacedCommand>(),
            [nameof(RecheckStatusOfServerCertificateReplacementCommand)] = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateReplacementCommand>(),
            [nameof(AddOrUpdateCompareExchangeCommand)] = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeCommand>(),
            [nameof(RemoveCompareExchangeCommand)] = GenerateJsonDeserializationRoutine<RemoveCompareExchangeCommand>(),
            [nameof(EditTimeSeriesConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditTimeSeriesConfigurationCommand>(),
            [nameof(EditRevisionsConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditRevisionsConfigurationCommand>(),
            [nameof(EditRevisionsForConflictsConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditRevisionsForConflictsConfigurationCommand>(),
            [nameof(EditDatabaseClientConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditDatabaseClientConfigurationCommand>(),
            [nameof(EditExpirationCommand)] = GenerateJsonDeserializationRoutine<EditExpirationCommand>(),
            [nameof(EditDataArchivalCommand)] = GenerateJsonDeserializationRoutine<EditDataArchivalCommand>(),
            [nameof(EditDocumentsCompressionCommand)] = GenerateJsonDeserializationRoutine<EditDocumentsCompressionCommand>(),
            [nameof(EditRefreshCommand)] = GenerateJsonDeserializationRoutine<EditRefreshCommand>(),
            [nameof(CreateNewShardCommand)] = GenerateJsonDeserializationRoutine<CreateNewShardCommand>(),
            [nameof(DeleteDatabaseCommand)] = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>(),
            [nameof(IncrementClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<IncrementClusterIdentityCommand>(),
            [nameof(IncrementClusterIdentitiesBatchCommand)] = GenerateJsonDeserializationRoutine<IncrementClusterIdentitiesBatchCommand>(),
            [nameof(UpdateClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<UpdateClusterIdentityCommand>(),
            [nameof(PutIndexCommand)] = GenerateJsonDeserializationRoutine<PutIndexCommand>(),
            [nameof(PutIndexesCommand)] = GenerateJsonDeserializationRoutine<PutIndexesCommand>(),
            [nameof(PutAutoIndexCommand)] = GenerateJsonDeserializationRoutine<PutAutoIndexCommand>(),
            [nameof(PutRollingIndexCommand)] = GenerateJsonDeserializationRoutine<PutRollingIndexCommand>(),
            [nameof(DeleteIndexCommand)] = GenerateJsonDeserializationRoutine<DeleteIndexCommand>(),
            [nameof(SetIndexLockCommand)] = GenerateJsonDeserializationRoutine<SetIndexLockCommand>(),
            [nameof(SetIndexPriorityCommand)] = GenerateJsonDeserializationRoutine<SetIndexPriorityCommand>(),
            [nameof(SetIndexStateCommand)] = GenerateJsonDeserializationRoutine<SetIndexStateCommand>(),
            [nameof(ModifyConflictSolverCommand)] = GenerateJsonDeserializationRoutine<ModifyConflictSolverCommand>(),
            [nameof(UpdateTopologyCommand)] = GenerateJsonDeserializationRoutine<UpdateTopologyCommand>(),
            [nameof(UpdateExternalReplicationCommand)] = GenerateJsonDeserializationRoutine<UpdateExternalReplicationCommand>(),
            [nameof(UpdatePullReplicationAsSinkCommand)] = GenerateJsonDeserializationRoutine<UpdatePullReplicationAsSinkCommand>(),
            [nameof(UpdatePullReplicationAsHubCommand)] = GenerateJsonDeserializationRoutine<UpdatePullReplicationAsHubCommand>(),
            [nameof(PromoteDatabaseNodeCommand)] = GenerateJsonDeserializationRoutine<PromoteDatabaseNodeCommand>(),
            [nameof(ToggleTaskStateCommand)] = GenerateJsonDeserializationRoutine<ToggleTaskStateCommand>(),
            [nameof(AddDatabaseCommand)] = GenerateJsonDeserializationRoutine<AddDatabaseCommand>(),
            [nameof(DeleteValueCommand)] = GenerateJsonDeserializationRoutine<DeleteValueCommand>(),
            [nameof(DeleteMultipleValuesCommand)] = GenerateJsonDeserializationRoutine<DeleteMultipleValuesCommand>(),
            [nameof(PutLicenseCommand)] = GenerateJsonDeserializationRoutine<PutLicenseCommand>(),
            [nameof(UpdateLicenseLimitsCommand)] = GenerateJsonDeserializationRoutine<UpdateLicenseLimitsCommand>(),
            [nameof(PutLicenseLimitsCommand)] = GenerateJsonDeserializationRoutine<PutLicenseLimitsCommand>(),
            [nameof(PutServerWideBackupConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutServerWideBackupConfigurationCommand>(),
            [nameof(DeleteServerWideBackupConfigurationCommand)] = GenerateJsonDeserializationRoutine<DeleteServerWideBackupConfigurationCommand>(),
            [nameof(DeleteCertificateFromClusterCommand)] = GenerateJsonDeserializationRoutine<DeleteCertificateFromClusterCommand>(),
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = GenerateJsonDeserializationRoutine<DeleteCertificateCollectionFromClusterCommand>(),
            [nameof(PutCertificateCommand)] = GenerateJsonDeserializationRoutine<PutCertificateCommand>(),
            [nameof(PutCertificateWithSamePinningHashCommand)] = GenerateJsonDeserializationRoutine<PutCertificateWithSamePinningHashCommand>(),
            [nameof(PutClientConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutClientConfigurationCommand>(),
            [nameof(PutServerWideStudioConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutServerWideStudioConfigurationCommand>(),
            [nameof(RemoveNodeFromDatabaseCommand)] = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>(),
            [nameof(AcknowledgeSubscriptionBatchCommand)] = GenerateJsonDeserializationRoutine<AcknowledgeSubscriptionBatchCommand>(),
            [nameof(RecordBatchSubscriptionDocumentsCommand)] = GenerateJsonDeserializationRoutine<RecordBatchSubscriptionDocumentsCommand>(),
            [nameof(PutSubscriptionCommand)] = GenerateJsonDeserializationRoutine<PutSubscriptionCommand>(),
            [nameof(PutShardedSubscriptionCommand)] = GenerateJsonDeserializationRoutine<PutShardedSubscriptionCommand>(),
            [nameof(ToggleSubscriptionStateCommand)] = GenerateJsonDeserializationRoutine<ToggleSubscriptionStateCommand>(),
            [nameof(DeleteSubscriptionCommand)] = GenerateJsonDeserializationRoutine<DeleteSubscriptionCommand>(),
            [nameof(UpdatePeriodicBackupCommand)] = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupCommand>(),
            [nameof(UpdatePeriodicBackupStatusCommand)] = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupStatusCommand>(),
            [nameof(AddRavenEtlCommand)] = GenerateJsonDeserializationRoutine<AddRavenEtlCommand>(),
            [nameof(AddSqlEtlCommand)] = GenerateJsonDeserializationRoutine<AddSqlEtlCommand>(),
            [nameof(AddOlapEtlCommand)] = GenerateJsonDeserializationRoutine<AddOlapEtlCommand>(),
            [nameof(AddElasticSearchEtlCommand)] = GenerateJsonDeserializationRoutine<AddElasticSearchEtlCommand>(),
            [nameof(AddQueueEtlCommand)] = GenerateJsonDeserializationRoutine<AddQueueEtlCommand>(),
            [nameof(AddQueueSinkCommand)] = GenerateJsonDeserializationRoutine<AddQueueSinkCommand>(),
            [nameof(UpdateRavenEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateRavenEtlCommand>(),
            [nameof(UpdateSqlEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateSqlEtlCommand>(),
            [nameof(UpdateOlapEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateOlapEtlCommand>(),
            [nameof(UpdateElasticSearchEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateElasticSearchEtlCommand>(),
            [nameof(UpdateQueueEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateQueueEtlCommand>(),
            [nameof(UpdateEtlProcessStateCommand)] = GenerateJsonDeserializationRoutine<UpdateEtlProcessStateCommand>(),
            [nameof(UpdateExternalReplicationStateCommand)] = GenerateJsonDeserializationRoutine<UpdateExternalReplicationStateCommand>(),
            [nameof(ShardedUpdateExternalReplicationStateCommand)] = GenerateJsonDeserializationRoutine<ShardedUpdateExternalReplicationStateCommand>(),
            [nameof(DeleteOngoingTaskCommand)] = GenerateJsonDeserializationRoutine<DeleteOngoingTaskCommand>(),
            [nameof(PutRavenConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutRavenConnectionStringCommand>(),
            [nameof(PutSqlConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutSqlConnectionStringCommand>(),
            [nameof(PutOlapConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutOlapConnectionStringCommand>(),
            [nameof(PutElasticSearchConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutElasticSearchConnectionStringCommand>(),
            [nameof(PutQueueConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutQueueConnectionStringCommand>(),
            [nameof(RemoveRavenConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveRavenConnectionStringCommand>(),
            [nameof(RemoveSqlConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveSqlConnectionStringCommand>(),
            [nameof(RemoveOlapConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveOlapConnectionStringCommand>(),
            [nameof(RemoveElasticSearchConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveElasticSearchConnectionStringCommand>(),
            [nameof(RemoveQueueConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveQueueConnectionStringCommand>(),
            [nameof(RemoveNodeFromClusterCommand)] = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>(),
            [nameof(UpdateSubscriptionClientConnectionTime)] = GenerateJsonDeserializationRoutine<UpdateSubscriptionClientConnectionTime>(),
            [nameof(UpdateSnmpDatabasesMappingCommand)] = GenerateJsonDeserializationRoutine<UpdateSnmpDatabasesMappingCommand>(),
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = GenerateJsonDeserializationRoutine<UpdateSnmpDatabaseIndexesMappingCommand>(),
            [nameof(RemoveEtlProcessStateCommand)] = GenerateJsonDeserializationRoutine<RemoveEtlProcessStateCommand>(),
            [nameof(PutSortersCommand)] = GenerateJsonDeserializationRoutine<PutSortersCommand>(),
            [nameof(DeleteSorterCommand)] = GenerateJsonDeserializationRoutine<DeleteSorterCommand>(),
            [nameof(PutAnalyzersCommand)] = GenerateJsonDeserializationRoutine<PutAnalyzersCommand>(),
            [nameof(DeleteAnalyzerCommand)] = GenerateJsonDeserializationRoutine<DeleteAnalyzerCommand>(),
            [nameof(UpdateUnusedDatabaseIdsCommand)] = GenerateJsonDeserializationRoutine<UpdateUnusedDatabaseIdsCommand>(),
            [nameof(ToggleDatabasesStateCommand)] = GenerateJsonDeserializationRoutine<ToggleDatabasesStateCommand>(),
            [nameof(PutServerWideExternalReplicationCommand)] = GenerateJsonDeserializationRoutine<PutServerWideExternalReplicationCommand>(),
            [nameof(DeleteServerWideTaskCommand)] = GenerateJsonDeserializationRoutine<DeleteServerWideTaskCommand>(),
            [nameof(ToggleServerWideTaskStateCommand)] = GenerateJsonDeserializationRoutine<ToggleServerWideTaskStateCommand>(),
            [nameof(PutServerWideAnalyzerCommand)] = GenerateJsonDeserializationRoutine<PutServerWideAnalyzerCommand>(),
            [nameof(DeleteServerWideAnalyzerCommand)] = GenerateJsonDeserializationRoutine<DeleteServerWideAnalyzerCommand>(),
            [nameof(PutServerWideSorterCommand)] = GenerateJsonDeserializationRoutine<PutServerWideSorterCommand>(),
            [nameof(DeleteServerWideSorterCommand)] = GenerateJsonDeserializationRoutine<DeleteServerWideSorterCommand>(),
            [nameof(DelayBackupCommand)] = GenerateJsonDeserializationRoutine<DelayBackupCommand>(),
            [nameof(EditLockModeCommand)] = GenerateJsonDeserializationRoutine<EditLockModeCommand>(),
            [nameof(EditPostgreSqlConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditPostgreSqlConfigurationCommand>(),
            [nameof(PutDatabaseStudioConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutDatabaseStudioConfigurationCommand>(),
            [nameof(PutDatabaseSettingsCommand)] = GenerateJsonDeserializationRoutine<PutDatabaseSettingsCommand>(),
            [nameof(PutDatabaseClientConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutDatabaseClientConfigurationCommand>(),
            [nameof(PutIndexHistoryCommand)] = GenerateJsonDeserializationRoutine<PutIndexHistoryCommand>(),
            [nameof(DeleteIndexHistoryCommand)] = GenerateJsonDeserializationRoutine<DeleteIndexHistoryCommand>(),
            [nameof(StartBucketMigrationCommand)] = GenerateJsonDeserializationRoutine<StartBucketMigrationCommand>(),
            [nameof(SourceMigrationSendCompletedCommand)] = GenerateJsonDeserializationRoutine<SourceMigrationSendCompletedCommand>(),
            [nameof(DestinationMigrationConfirmCommand)] = GenerateJsonDeserializationRoutine<DestinationMigrationConfirmCommand>(),
            [nameof(SourceMigrationCleanupCommand)] = GenerateJsonDeserializationRoutine<SourceMigrationCleanupCommand>(),
            [nameof(UpdateServerPublishedUrlsCommand)] = GenerateJsonDeserializationRoutine<UpdateServerPublishedUrlsCommand>(),
            [nameof(AddQueueSinkCommand)] = GenerateJsonDeserializationRoutine<AddQueueSinkCommand>(),
            [nameof(UpdateQueueSinkCommand)] = GenerateJsonDeserializationRoutine<UpdateQueueSinkCommand>(),
            [nameof(UpdateQueueSinkProcessStateCommand)] = GenerateJsonDeserializationRoutine<UpdateQueueSinkProcessStateCommand>(),
            [nameof(RemoveQueueSinkProcessStateCommand)] = GenerateJsonDeserializationRoutine<RemoveQueueSinkProcessStateCommand>(),
            [nameof(UpdateResponsibleNodeForTasksCommand)] = GenerateJsonDeserializationRoutine<UpdateResponsibleNodeForTasksCommand>(),
            [nameof(AddPrefixedShardingSettingCommand)] = GenerateJsonDeserializationRoutine<AddPrefixedShardingSettingCommand>(),
            [nameof(DeletePrefixedShardingSettingCommand)] = GenerateJsonDeserializationRoutine<DeletePrefixedShardingSettingCommand>(),
            [nameof(UpdatePrefixedShardingSettingCommand)] = GenerateJsonDeserializationRoutine<UpdatePrefixedShardingSettingCommand>()
        };
    }
}
