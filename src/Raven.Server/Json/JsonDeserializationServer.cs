using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using FacetSetup = Raven.Client.Documents.Queries.Facets.FacetSetup;
using Raven.Server.Documents.Replication;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Migration;
using Raven.Server.SqlMigration.Model;
using Raven.Server.Web.Studio;
using Raven.Server.Web.System;

namespace Raven.Server.Json
{
    internal sealed class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, StartTransactionsRecordingOperation.Parameters> StartTransactionsRecordingOperationParameters = GenerateJsonDeserializationRoutine<StartTransactionsRecordingOperation.Parameters>();

        public static readonly Func<BlittableJsonReaderObject, ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader> NodeDebugInfoRequestHeader = GenerateJsonDeserializationRoutine<ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatusReport> DatabaseStatusReport = GenerateJsonDeserializationRoutine<DatabaseStatusReport>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseSmugglerOptionsServerSide> DatabaseSmugglerOptions = GenerateJsonDeserializationRoutine<DatabaseSmugglerOptionsServerSide>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationMessageReply> ReplicationMessageReply = GenerateJsonDeserializationRoutine<ReplicationMessageReply>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationInitialRequest> ReplicationInitialRequest = GenerateJsonDeserializationRoutine<ReplicationInitialRequest>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionWorkerOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionWorkerOptions>();

        public static readonly Func<BlittableJsonReaderObject, ConflictSolver> ConflictSolver = GenerateJsonDeserializationRoutine<ConflictSolver>();
        public static readonly Func<BlittableJsonReaderObject, ScriptResolver> ScriptResolver = GenerateJsonDeserializationRoutine<ScriptResolver>();

        public static readonly Func<BlittableJsonReaderObject, TestSqlEtlScript> TestSqlEtlScript = GenerateJsonDeserializationRoutine<TestSqlEtlScript>();

        public static readonly Func<BlittableJsonReaderObject, TestRavenEtlScript> TestRavenEtlScript = GenerateJsonDeserializationRoutine<TestRavenEtlScript>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionCreationOptions> SubscriptionCreationParams = GenerateJsonDeserializationRoutine<SubscriptionCreationOptions>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionTryout> SubscriptionTryout = GenerateJsonDeserializationRoutine<SubscriptionTryout>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsConfiguration> RevisionsConfiguration = GenerateJsonDeserializationRoutine<RevisionsConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseRestorePath> DatabaseRestorePath = GenerateJsonDeserializationRoutine<DatabaseRestorePath>();

        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, SorterDefinition> SorterDefinition = GenerateJsonDeserializationRoutine<SorterDefinition>();

        public static readonly Func<BlittableJsonReaderObject, AutoIndexDefinition> AutoIndexDefinition = GenerateJsonDeserializationRoutine<AutoIndexDefinition>();

        internal static readonly Func<BlittableJsonReaderObject, LegacyIndexDefinition> LegacyIndexDefinition = GenerateJsonDeserializationRoutine<LegacyIndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, FacetSetup> FacetSetup = GenerateJsonDeserializationRoutine<FacetSetup>();

        public static readonly Func<BlittableJsonReaderObject, LatestVersionCheck.VersionInfo> LatestVersionCheckVersionInfo = GenerateJsonDeserializationRoutine<LatestVersionCheck.VersionInfo>();

        public static readonly Func<BlittableJsonReaderObject, License> License = GenerateJsonDeserializationRoutine<License>();
        
        public static readonly Func<BlittableJsonReaderObject, SetupSettings> SetupSettings = GenerateJsonDeserializationRoutine<SetupSettings>();

        public static readonly Func<BlittableJsonReaderObject, LicenseInfo> LicenseInfo = GenerateJsonDeserializationRoutine<LicenseInfo>();

        public static readonly Func<BlittableJsonReaderObject, LicenseLimits> LicenseLimits = GenerateJsonDeserializationRoutine<LicenseLimits>();

        public static readonly Func<BlittableJsonReaderObject, LeasedLicense> LeasedLicense = GenerateJsonDeserializationRoutine<LeasedLicense>();

        public static readonly Func<BlittableJsonReaderObject, LicenseSupportInfo> LicenseSupportInfo = GenerateJsonDeserializationRoutine<LicenseSupportInfo>();

        public static readonly Func<BlittableJsonReaderObject, UserRegistrationInfo> UserRegistrationInfo = GenerateJsonDeserializationRoutine<UserRegistrationInfo>();

        public static readonly Func<BlittableJsonReaderObject, FeedbackForm> FeedbackForm = GenerateJsonDeserializationRoutine<FeedbackForm>();

        public static readonly Func<BlittableJsonReaderObject, CertificateDefinition> CertificateDefinition = GenerateJsonDeserializationRoutine<CertificateDefinition>();

        public static readonly Func<BlittableJsonReaderObject, UserDomainsWithIps> UserDomainsWithIps = GenerateJsonDeserializationRoutine<UserDomainsWithIps>();

        public static readonly Func<BlittableJsonReaderObject, SetupInfo> SetupInfo = GenerateJsonDeserializationRoutine<SetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, ContinueSetupInfo> ContinueSetupInfo = GenerateJsonDeserializationRoutine<ContinueSetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, UnsecuredSetupInfo> UnsecuredSetupInfo = GenerateJsonDeserializationRoutine<UnsecuredSetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, SourceSqlDatabase> SourceSqlDatabase = GenerateJsonDeserializationRoutine<SourceSqlDatabase>();

        public static readonly Func<BlittableJsonReaderObject, RestoreSettings> RestoreSettings = GenerateJsonDeserializationRoutine<RestoreSettings>();

        public static readonly Func<BlittableJsonReaderObject, CompactSettings> CompactSettings = GenerateJsonDeserializationRoutine<CompactSettings>();

        public static readonly Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, StudioConfiguration> StudioConfiguration = GenerateJsonDeserializationRoutine<StudioConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ServerWideStudioConfiguration> ServerWideStudioConfiguration = GenerateJsonDeserializationRoutine<ServerWideStudioConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, IndexQueryServerSide> IndexQuery = GenerateJsonDeserializationRoutine<IndexQueryServerSide>();

        public static readonly Func<BlittableJsonReaderObject, SingleDatabaseMigrationConfiguration> SingleDatabaseMigrationConfiguration = GenerateJsonDeserializationRoutine<SingleDatabaseMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, DatabasesMigrationConfiguration> DatabasesMigrationConfiguration = GenerateJsonDeserializationRoutine<DatabasesMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, OfflineMigrationConfiguration> OfflineMigrationConfiguration = GenerateJsonDeserializationRoutine<OfflineMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, MigrationConfiguration> MigrationConfiguration = GenerateJsonDeserializationRoutine<MigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, LastEtagsInfo> OperationState = GenerateJsonDeserializationRoutine<LastEtagsInfo>();

        public static readonly Func<BlittableJsonReaderObject, ImportInfo> ImportInfo = GenerateJsonDeserializationRoutine<ImportInfo>();

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisOptions> MoreLikeThisOptions = GenerateJsonDeserializationRoutine<MoreLikeThisOptions>();

        public static readonly Func<BlittableJsonReaderObject, FacetOptions> FacetOptions = GenerateJsonDeserializationRoutine<FacetOptions>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseInfo> DatabaseInfo = GenerateJsonDeserializationRoutine<DatabaseInfo>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTopologyChanged> ClusterTopologyChanged = GenerateJsonDeserializationRoutine<ClusterTopologyChanged>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTransactionCommand.ClusterTransactionDataCommand> ClusterTransactionDataCommand = GenerateJsonDeserializationRoutine<ClusterTransactionCommand.ClusterTransactionDataCommand>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTransactionCommand.ClusterTransactionOptions> ClusterTransactionOptions = GenerateJsonDeserializationRoutine<ClusterTransactionCommand.ClusterTransactionOptions>();

        public static readonly Func<BlittableJsonReaderObject, NodeConnectionTestResult> NodeConnectionTestResult = GenerateJsonDeserializationRoutine<NodeConnectionTestResult>();

        public static readonly Func<BlittableJsonReaderObject, SingleNodeDataDirectoryResult> SingleNodeDataDirectoryResult = GenerateJsonDeserializationRoutine<SingleNodeDataDirectoryResult>();

        public class Parameters
        {
            private Parameters()
            {
            }

            public static readonly Func<BlittableJsonReaderObject, DeleteDatabasesOperation.Parameters> DeleteDatabasesParameters = GenerateJsonDeserializationRoutine<DeleteDatabasesOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, ReorderDatabaseMembersOperation.Parameters> MembersOrder = GenerateJsonDeserializationRoutine<ReorderDatabaseMembersOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, ToggleDatabasesStateOperation.Parameters> DisableDatabaseToggleParameters = GenerateJsonDeserializationRoutine<ToggleDatabasesStateOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, SetIndexesLockOperation.Parameters> SetIndexLockParameters = GenerateJsonDeserializationRoutine<SetIndexesLockOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, SetIndexesPriorityOperation.Parameters> SetIndexPriorityParameters = GenerateJsonDeserializationRoutine<SetIndexesPriorityOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, SetLogsConfigurationOperation.Parameters> SetLogsConfigurationParameters = GenerateJsonDeserializationRoutine<SetLogsConfigurationOperation.Parameters>();

            public static readonly Func<BlittableJsonReaderObject, AdminRevisionsHandler.Parameters> DeleteRevisionsParameters = GenerateJsonDeserializationRoutine<AdminRevisionsHandler.Parameters>();
        }
    }
}
