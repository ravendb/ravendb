using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Migration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Client.ServerWide.Operations.Migration;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.Indexes;
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
using Raven.Server.Smuggler.Migration;
using Raven.Server.Web.System;

namespace Raven.Server.Json
{
    internal sealed class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader> NodeDebugInfoRequestHeader = GenerateJsonDeserializationRoutine<ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatusReport> DatabaseStatusReport = GenerateJsonDeserializationRoutine<DatabaseStatusReport>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo = GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseSmugglerOptionsServerSide> DatabaseSmugglerOptions = GenerateJsonDeserializationRoutine<DatabaseSmugglerOptionsServerSide>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationMessageReply> ReplicationMessageReply = GenerateJsonDeserializationRoutine<ReplicationMessageReply>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionWorkerOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionWorkerOptions>();

        public static readonly Func<BlittableJsonReaderObject, ConflictSolver> ConflictSolver = GenerateJsonDeserializationRoutine<ConflictSolver>();
        public static readonly Func<BlittableJsonReaderObject, ScriptResolver> ScriptResolver = GenerateJsonDeserializationRoutine<ScriptResolver>();

        public static readonly Func<BlittableJsonReaderObject, EtlProcessState> EtlProcessStatus = GenerateJsonDeserializationRoutine<EtlProcessState>();

        public static readonly Func<BlittableJsonReaderObject, SimulateSqlEtl> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlEtl>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionCreationOptions> SubscriptionCreationParams = GenerateJsonDeserializationRoutine<SubscriptionCreationOptions>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionTryout> SubscriptionTryout = GenerateJsonDeserializationRoutine<SubscriptionTryout>();

        public static readonly Func<BlittableJsonReaderObject, RevisionsConfiguration> RevisionsConfiguration = GenerateJsonDeserializationRoutine<RevisionsConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ExpirationConfiguration> ExpirationConfiguration = GenerateJsonDeserializationRoutine<ExpirationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupConfiguration> PeriodicBackupConfiguration = GenerateJsonDeserializationRoutine<PeriodicBackupConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, PeriodicBackupStatus> PeriodicBackupStatus = GenerateJsonDeserializationRoutine<PeriodicBackupStatus>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseRestorePath> DatabaseRestorePath = GenerateJsonDeserializationRoutine<DatabaseRestorePath>();

        public static readonly Func<BlittableJsonReaderObject, IndexDefinition> IndexDefinition = GenerateJsonDeserializationRoutine<IndexDefinition>();

        internal static readonly Func<BlittableJsonReaderObject, LegacyIndexDefinition> LegacyIndexDefinition = GenerateJsonDeserializationRoutine<LegacyIndexDefinition>();

        public static readonly Func<BlittableJsonReaderObject, FacetSetup> FacetSetup = GenerateJsonDeserializationRoutine<FacetSetup>();

        public static readonly Func<BlittableJsonReaderObject, LatestVersionCheck.VersionInfo> LatestVersionCheckVersionInfo = GenerateJsonDeserializationRoutine<LatestVersionCheck.VersionInfo>();

        public static readonly Func<BlittableJsonReaderObject, License> License = GenerateJsonDeserializationRoutine<License>();

        public static readonly Func<BlittableJsonReaderObject, LicenseInfo> LicenseInfo = GenerateJsonDeserializationRoutine<LicenseInfo>();

        public static readonly Func<BlittableJsonReaderObject, LicenseLimits> LicenseLimits = GenerateJsonDeserializationRoutine<LicenseLimits>();

        public static readonly Func<BlittableJsonReaderObject, LeasedLicense> LeasedLicense = GenerateJsonDeserializationRoutine<LeasedLicense>();

        public static readonly Func<BlittableJsonReaderObject, LicenseSupportInfo> LicenseSupportInfo = GenerateJsonDeserializationRoutine<LicenseSupportInfo>();

        public static readonly Func<BlittableJsonReaderObject, UserRegistrationInfo> UserRegistrationInfo = GenerateJsonDeserializationRoutine<UserRegistrationInfo>();

        public static readonly Func<BlittableJsonReaderObject, FeedbackForm> FeedbackForm = GenerateJsonDeserializationRoutine<FeedbackForm>();

        public static readonly Func<BlittableJsonReaderObject, CustomIndexPaths> CustomIndexPaths = GenerateJsonDeserializationRoutine<CustomIndexPaths>();

        public static readonly Func<BlittableJsonReaderObject, CertificateDefinition> CertificateDefinition = GenerateJsonDeserializationRoutine<CertificateDefinition>();

        public static readonly Func<BlittableJsonReaderObject, UserDomainsResult> UserDomainsResult = GenerateJsonDeserializationRoutine<UserDomainsResult>();

        public static readonly Func<BlittableJsonReaderObject, UserDomainsWithIps> UserDomainsWithIps = GenerateJsonDeserializationRoutine<UserDomainsWithIps>();

        public static readonly Func<BlittableJsonReaderObject, SetupInfo> SetupInfo = GenerateJsonDeserializationRoutine<SetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, ContinueSetupInfo> ContinueSetupInfo = GenerateJsonDeserializationRoutine<ContinueSetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, UnsecuredSetupInfo> UnsecuredSetupInfo = GenerateJsonDeserializationRoutine<UnsecuredSetupInfo>();

        public static readonly Func<BlittableJsonReaderObject, ClaimDomainInfo> ClaimDomainInfo = GenerateJsonDeserializationRoutine<ClaimDomainInfo>();

        public static readonly Func<BlittableJsonReaderObject, ListDomainsInfo> ListDomainsInfo = GenerateJsonDeserializationRoutine<ListDomainsInfo>();

        public static readonly Func<BlittableJsonReaderObject, RestoreSettings> RestoreSettings = GenerateJsonDeserializationRoutine<RestoreSettings>();

        public static readonly Func<BlittableJsonReaderObject, CompactSettings> CompactSettings = GenerateJsonDeserializationRoutine<CompactSettings>();

        public static readonly Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, IndexQueryServerSide> IndexQuery = GenerateJsonDeserializationRoutine<IndexQueryServerSide>();

        public static readonly Func<BlittableJsonReaderObject, SingleDatabaseMigrationConfiguration> SingleDatabaseMigrationConfiguration = GenerateJsonDeserializationRoutine<SingleDatabaseMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, DatabasesMigrationConfiguration> DatabasesMigrationConfiguration = GenerateJsonDeserializationRoutine<DatabasesMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, OfflineMigrationConfiguration> OfflineMigrationConfiguration = GenerateJsonDeserializationRoutine<OfflineMigrationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, LastEtagsInfo> OperationState = GenerateJsonDeserializationRoutine<LastEtagsInfo>();

        public static readonly Func<BlittableJsonReaderObject, ImportInfo> ImportInfo = GenerateJsonDeserializationRoutine<ImportInfo>();

        public static readonly Func<BlittableJsonReaderObject, SqlMigrationImportOperation.SqlMigrationTable> SqlMigrationTable = SqlMigrationImportOperation.SqlMigrationTable.ManualDeserializationFunc;

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisOptions> MoreLikeThisOptions = GenerateJsonDeserializationRoutine<MoreLikeThisOptions>();

        public static readonly Func<BlittableJsonReaderObject, FacetOptions> FacetOptions = GenerateJsonDeserializationRoutine<FacetOptions>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseInfo> DatabaseInfo = GenerateJsonDeserializationRoutine<DatabaseInfo>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTopologyChanged> ClusterTopologyChanged = GenerateJsonDeserializationRoutine<ClusterTopologyChanged>();

        public static readonly Func<BlittableJsonReaderObject, NodeConnectionTestResult> NodeConnectionTestResult = GenerateJsonDeserializationRoutine<NodeConnectionTestResult>();

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
