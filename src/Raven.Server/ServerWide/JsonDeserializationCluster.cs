using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, UpdateClusterIdentityCommand> UpdateClusterIdentityCommand = GenerateJsonDeserializationRoutine<UpdateClusterIdentityCommand>();

        public static readonly Func<BlittableJsonReaderObject, IncrementClusterIdentityCommand> IncrementClusterIdentityCommand = GenerateJsonDeserializationRoutine<IncrementClusterIdentityCommand>();

        public static readonly Func<BlittableJsonReaderObject, IncrementClusterIdentitiesBatchCommand> IncrementClusterIdentitiesBatchCommand = GenerateJsonDeserializationRoutine<IncrementClusterIdentitiesBatchCommand>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionState> SubscriptionState = GenerateJsonDeserializationRoutine<SubscriptionState>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteMultipleValuesCommand> DeleteMultipleValuesCommand = GenerateJsonDeserializationRoutine<DeleteMultipleValuesCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseTopology> DatabaseTopology = GenerateJsonDeserializationRoutine<DatabaseTopology>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromDatabaseCommand> RemoveNodeFromDatabaseCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromClusterCommand> RemoveNodeFromClusterCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupConfiguration> PeriodicBackupConfiguration = GenerateJsonDeserializationRoutine<PeriodicBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ExternalReplicationState> ExternalReplicationState = GenerateJsonDeserializationRoutine<ExternalReplicationState>();

        public static readonly Func<BlittableJsonReaderObject, RestoreBackupConfiguration> RestoreBackupConfiguration = GenerateJsonDeserializationRoutine<RestoreBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsConfiguration> RevisionsConfiguration = GenerateJsonDeserializationRoutine<RevisionsConfiguration>();

        public static Func<BlittableJsonReaderObject, RavenEtlConfiguration> RavenEtlConfiguration = GenerateJsonDeserializationRoutine<RavenEtlConfiguration>();

        public static Func<BlittableJsonReaderObject, SqlEtlConfiguration> SqlEtlConfiguration = GenerateJsonDeserializationRoutine<SqlEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ServerStore.PutRaftCommandResult> PutRaftCommandResult = GenerateJsonDeserializationRoutine<ServerStore.PutRaftCommandResult>();

        public static readonly Func<BlittableJsonReaderObject, AddOrUpdateCompareExchangeCommand.CompareExchangeResult> CompareExchangeResult = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeCommand.CompareExchangeResult>();

        public static readonly Func<BlittableJsonReaderObject, AdminJsScript> AdminJsScript = GenerateJsonDeserializationRoutine<AdminJsScript>();

        public static Func<BlittableJsonReaderObject, RavenConnectionString> RavenConnectionString = GenerateJsonDeserializationRoutine<RavenConnectionString>();

        public static Func<BlittableJsonReaderObject, SqlConnectionString> SqlConnectionString = GenerateJsonDeserializationRoutine<SqlConnectionString>();
        
        public static Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();
        
        public static Func<BlittableJsonReaderObject, InstallUpdatedServerCertificateCommand> InstallUpdatedServerCertificateCommand = GenerateJsonDeserializationRoutine<InstallUpdatedServerCertificateCommand>();
        public static Func<BlittableJsonReaderObject, ConfirmReceiptServerCertificateCommand> ConfirmReceiptServerCertificateCommand = GenerateJsonDeserializationRoutine<ConfirmReceiptServerCertificateCommand>();
        public static Func<BlittableJsonReaderObject, RecheckStatusOfServerCertificateCommand> RecheckStatusOfServerCertificateCommand = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateCommand>();
        public static Func<BlittableJsonReaderObject, ConfirmServerCertificateReplacedCommand> ConfirmServerCertificateReplacedCommand = GenerateJsonDeserializationRoutine<ConfirmServerCertificateReplacedCommand>();
        public static Func<BlittableJsonReaderObject, RecheckStatusOfServerCertificateReplacementCommand> RecheckStatusOfServerCertificateReplacementCommand = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateReplacementCommand>();

        public static Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>> Commands = new Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>>
        {
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeBatchCommand>(),
            [nameof(CleanUpClusterStateCommand)] = GenerateJsonDeserializationRoutine<CleanUpClusterStateCommand>(),
            [nameof(ClusterTransactionCommand)] = GenerateJsonDeserializationRoutine<ClusterTransactionCommand>(),
            [nameof(InstallUpdatedServerCertificateCommand)] = GenerateJsonDeserializationRoutine<InstallUpdatedServerCertificateCommand>(),
            [nameof(ConfirmReceiptServerCertificateCommand)] = GenerateJsonDeserializationRoutine<ConfirmReceiptServerCertificateCommand>(),
            [nameof(RecheckStatusOfServerCertificateCommand)] = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateCommand>(),
            [nameof(ConfirmServerCertificateReplacedCommand)] = GenerateJsonDeserializationRoutine<ConfirmServerCertificateReplacedCommand>(),
            [nameof(RecheckStatusOfServerCertificateReplacementCommand)] = GenerateJsonDeserializationRoutine<RecheckStatusOfServerCertificateReplacementCommand>(),
            [nameof(AddOrUpdateCompareExchangeCommand)] = GenerateJsonDeserializationRoutine<AddOrUpdateCompareExchangeCommand>(),
            [nameof(RemoveCompareExchangeCommand)] = GenerateJsonDeserializationRoutine<RemoveCompareExchangeCommand>(),
            [nameof(EditRevisionsConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditRevisionsConfigurationCommand>(),
            [nameof(EditExpirationCommand)] = GenerateJsonDeserializationRoutine<EditExpirationCommand>(),
            [nameof(DeleteDatabaseCommand)] = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>(),
            [nameof(IncrementClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<IncrementClusterIdentityCommand>(),
            [nameof(IncrementClusterIdentitiesBatchCommand)] = GenerateJsonDeserializationRoutine<IncrementClusterIdentitiesBatchCommand>(),
            [nameof(UpdateClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<UpdateClusterIdentityCommand>(),
            [nameof(PutIndexCommand)] = GenerateJsonDeserializationRoutine<PutIndexCommand>(),
            [nameof(PutIndexesCommand)] = GenerateJsonDeserializationRoutine<PutIndexesCommand>(),
            [nameof(PutAutoIndexCommand)] = GenerateJsonDeserializationRoutine<PutAutoIndexCommand>(),
            [nameof(DeleteIndexCommand)] = GenerateJsonDeserializationRoutine<DeleteIndexCommand>(),
            [nameof(SetIndexLockCommand)] = GenerateJsonDeserializationRoutine<SetIndexLockCommand>(),
            [nameof(SetIndexPriorityCommand)] = GenerateJsonDeserializationRoutine<SetIndexPriorityCommand>(),
            [nameof(SetIndexStateCommand)] = GenerateJsonDeserializationRoutine<SetIndexStateCommand>(),
            [nameof(ModifyConflictSolverCommand)] = GenerateJsonDeserializationRoutine<ModifyConflictSolverCommand>(),
            [nameof(UpdateTopologyCommand)] = GenerateJsonDeserializationRoutine<UpdateTopologyCommand>(),
            [nameof(UpdateExternalReplicationCommand)] = GenerateJsonDeserializationRoutine<UpdateExternalReplicationCommand>(),
            [nameof(PromoteDatabaseNodeCommand)] = GenerateJsonDeserializationRoutine<PromoteDatabaseNodeCommand>(),
            [nameof(ToggleTaskStateCommand)] = GenerateJsonDeserializationRoutine<ToggleTaskStateCommand>(),
            [nameof(AddDatabaseCommand)] = GenerateJsonDeserializationRoutine<AddDatabaseCommand>(),
            [nameof(DeleteValueCommand)] = GenerateJsonDeserializationRoutine<DeleteValueCommand>(),
            [nameof(DeleteMultipleValuesCommand)] = GenerateJsonDeserializationRoutine<DeleteMultipleValuesCommand>(),
            [nameof(PutLicenseCommand)] = GenerateJsonDeserializationRoutine<PutLicenseCommand>(),
            [nameof(PutLicenseLimitsCommand)] = GenerateJsonDeserializationRoutine<PutLicenseLimitsCommand>(),
            [nameof(DeleteCertificateFromClusterCommand)] = GenerateJsonDeserializationRoutine<DeleteCertificateFromClusterCommand>(),
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = GenerateJsonDeserializationRoutine<DeleteCertificateCollectionFromClusterCommand>(),
            [nameof(PutCertificateCommand)] = GenerateJsonDeserializationRoutine<PutCertificateCommand>(),
            [nameof(PutClientConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutClientConfigurationCommand>(),
            [nameof(PutServerWideStudioConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutServerWideStudioConfigurationCommand>(),
            [nameof(RemoveNodeFromDatabaseCommand)] = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>(),
            [nameof(AcknowledgeSubscriptionBatchCommand)] = GenerateJsonDeserializationRoutine<AcknowledgeSubscriptionBatchCommand>(),
            [nameof(PutSubscriptionCommand)] = GenerateJsonDeserializationRoutine<PutSubscriptionCommand>(),
            [nameof(ToggleSubscriptionStateCommand)] = GenerateJsonDeserializationRoutine<ToggleSubscriptionStateCommand>(),
            [nameof(DeleteSubscriptionCommand)] = GenerateJsonDeserializationRoutine<DeleteSubscriptionCommand>(),
            [nameof(UpdatePeriodicBackupCommand)] = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupCommand>(),
            [nameof(UpdatePeriodicBackupStatusCommand)] = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupStatusCommand>(),
            [nameof(AddRavenEtlCommand)] = GenerateJsonDeserializationRoutine<AddRavenEtlCommand>(),
            [nameof(AddSqlEtlCommand)] = GenerateJsonDeserializationRoutine<AddSqlEtlCommand>(),
            [nameof(UpdateRavenEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateRavenEtlCommand>(),
            [nameof(UpdateSqlEtlCommand)] = GenerateJsonDeserializationRoutine<UpdateSqlEtlCommand>(),
            [nameof(UpdateEtlProcessStateCommand)] = GenerateJsonDeserializationRoutine<UpdateEtlProcessStateCommand>(),
            [nameof(UpdateExternalReplicationStateCommand)] = GenerateJsonDeserializationRoutine<UpdateExternalReplicationStateCommand>(),
            [nameof(DeleteOngoingTaskCommand)] = GenerateJsonDeserializationRoutine<DeleteOngoingTaskCommand>(),
            [nameof(PutRavenConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutRavenConnectionStringCommand>(),
            [nameof(PutSqlConnectionStringCommand)] = GenerateJsonDeserializationRoutine<PutSqlConnectionStringCommand>(),
            [nameof(RemoveRavenConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveRavenConnectionStringCommand>(),
            [nameof(RemoveSqlConnectionStringCommand)] = GenerateJsonDeserializationRoutine<RemoveSqlConnectionStringCommand>(),
            [nameof(RemoveNodeFromClusterCommand)] = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>(),
            [nameof(UpdateSubscriptionClientConnectionTime)] = GenerateJsonDeserializationRoutine<UpdateSubscriptionClientConnectionTime>(),
            [nameof(UpdateSnmpDatabasesMappingCommand)] = GenerateJsonDeserializationRoutine<UpdateSnmpDatabasesMappingCommand>(),
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = GenerateJsonDeserializationRoutine<UpdateSnmpDatabaseIndexesMappingCommand>(),
            [nameof(RemoveEtlProcessStateCommand)] = GenerateJsonDeserializationRoutine<RemoveEtlProcessStateCommand>()
        };
    }
}
