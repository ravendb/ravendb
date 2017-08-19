using System;
using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Expiration;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Revisions;
using Raven.Server.Commercial;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, SubscriptionState> SubscriptionState = GenerateJsonDeserializationRoutine<SubscriptionState>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteDatabaseCommand> DeleteDatabaseCommand = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromDatabaseCommand> RemoveNodeFromDatabaseCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromClusterCommand> RemoveNodeFromClusterCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupConfiguration> PeriodicBackupConfiguration = GenerateJsonDeserializationRoutine<PeriodicBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RestoreBackupConfiguration> RestoreBackupConfiguration = GenerateJsonDeserializationRoutine<RestoreBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsConfiguration> RevisionsConfiguration = GenerateJsonDeserializationRoutine<RevisionsConfiguration>();

        public static Func<BlittableJsonReaderObject, RavenEtlConfiguration> RavenEtlConfiguration = GenerateJsonDeserializationRoutine<RavenEtlConfiguration>();

        public static Func<BlittableJsonReaderObject, SqlEtlConfiguration> SqlEtlConfiguration = GenerateJsonDeserializationRoutine<SqlEtlConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ServerStore.PutRaftCommandResult> PutRaftCommandResult = GenerateJsonDeserializationRoutine<ServerStore.PutRaftCommandResult>();

        public static readonly Func<BlittableJsonReaderObject, AdminJsScript> AdminJsScript = GenerateJsonDeserializationRoutine<AdminJsScript>();

        public static Func<BlittableJsonReaderObject, RavenConnectionString> RavenConnectionString = GenerateJsonDeserializationRoutine<RavenConnectionString>();

        public static Func<BlittableJsonReaderObject, SqlConnectionString> SqlConnectionString = GenerateJsonDeserializationRoutine<SqlConnectionString>();

        public static Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>> Commands = new Dictionary<string, Func<BlittableJsonReaderObject, CommandBase>>
        {
            [nameof(EditRevisionsConfigurationCommand)] = GenerateJsonDeserializationRoutine<EditRevisionsConfigurationCommand>(),
            [nameof(EditExpirationCommand)] = GenerateJsonDeserializationRoutine<EditExpirationCommand>(),
            [nameof(DeleteDatabaseCommand)] = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>(),
            [nameof(IncrementClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<IncrementClusterIdentityCommand>(),
            [nameof(UpdateClusterIdentityCommand)] = GenerateJsonDeserializationRoutine<UpdateClusterIdentityCommand>(),
            [nameof(ModifyCustomFunctionsCommand)] = GenerateJsonDeserializationRoutine<ModifyCustomFunctionsCommand>(),
            [nameof(PutIndexCommand)] = GenerateJsonDeserializationRoutine<PutIndexCommand>(),
            [nameof(PutAutoIndexCommand)] = GenerateJsonDeserializationRoutine<PutAutoIndexCommand>(),
            [nameof(DeleteIndexCommand)] = GenerateJsonDeserializationRoutine<DeleteIndexCommand>(),
            [nameof(SetIndexLockCommand)] = GenerateJsonDeserializationRoutine<SetIndexLockCommand>(),
            [nameof(SetIndexPriorityCommand)] = GenerateJsonDeserializationRoutine<SetIndexPriorityCommand>(),
            [nameof(ModifyConflictSolverCommand)] = GenerateJsonDeserializationRoutine<ModifyConflictSolverCommand>(),
            [nameof(UpdateTopologyCommand)] = GenerateJsonDeserializationRoutine<UpdateTopologyCommand>(),
            [nameof(UpdateExternalReplicationCommand)] = GenerateJsonDeserializationRoutine<UpdateExternalReplicationCommand>(),
            [nameof(PromoteDatabaseNodeCommand)] = GenerateJsonDeserializationRoutine<PromoteDatabaseNodeCommand>(),
            [nameof(ToggleTaskStateCommand)] = GenerateJsonDeserializationRoutine<ToggleTaskStateCommand>(),
            [nameof(AddDatabaseCommand)] = GenerateJsonDeserializationRoutine<AddDatabaseCommand>(),
            [nameof(DeleteValueCommand)] = GenerateJsonDeserializationRoutine<DeleteValueCommand>(),
            [nameof(PutLicenseCommand)] = GenerateJsonDeserializationRoutine<PutLicenseCommand>(),
            [nameof(DeactivateLicenseCommand)] = GenerateJsonDeserializationRoutine<DeactivateLicenseCommand>(),
            [nameof(PutCertificateCommand)] = GenerateJsonDeserializationRoutine<PutCertificateCommand>(),
            [nameof(PutClientConfigurationCommand)] = GenerateJsonDeserializationRoutine<PutClientConfigurationCommand>(),
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
            [nameof(DeleteOngoingTaskCommand)] = GenerateJsonDeserializationRoutine<DeleteOngoingTaskCommand>(),
            [nameof(PutRavenConnectionString)] = GenerateJsonDeserializationRoutine<PutRavenConnectionString>(),
            [nameof(PutSqlConnectionString)] = GenerateJsonDeserializationRoutine<PutSqlConnectionString>(),
            [nameof(RemoveRavenConnectionString)] = GenerateJsonDeserializationRoutine<RemoveRavenConnectionString>(),
            [nameof(RemoveSqlConnectionString)] = GenerateJsonDeserializationRoutine<RemoveSqlConnectionString>(),
            [nameof(RemoveNodeFromClusterCommand)] = GenerateJsonDeserializationRoutine<RemoveNodeFromClusterCommand>()
        };
    }
}
