using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Expiration;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Revisions;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide.BackgroundTasks;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Facet = Raven.Client.Documents.Queries.Facets.Facet;
using FacetSetup = Raven.Client.Documents.Queries.Facets.FacetSetup;
using Raven.Server.Documents.Replication;

namespace Raven.Server.Json
{
    internal sealed class JsonDeserializationServer : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader> NodeDebugInfoRequestHeader =
            GenerateJsonDeserializationRoutine<ServerWideDebugInfoPackageHandler.NodeDebugInfoRequestHeader>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatusReport> DatabaseStatusReport = GenerateJsonDeserializationRoutine<DatabaseStatusReport>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo = GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseSmugglerOptions> DatabaseSmugglerOptions = GenerateJsonDeserializationRoutine<DatabaseSmugglerOptions>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationMessageReply> ReplicationMessageReply = GenerateJsonDeserializationRoutine<ReplicationMessageReply>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationLatestEtagRequest> ReplicationLatestEtagRequest = GenerateJsonDeserializationRoutine<ReplicationLatestEtagRequest>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionClientMessage> SubscriptionConnectionClientMessage = GenerateJsonDeserializationRoutine<SubscriptionConnectionClientMessage>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionConnectionOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();

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
        public static readonly Func<BlittableJsonReaderObject, Facet> Facet = GenerateJsonDeserializationRoutine<Facet>();

        public static readonly Func<BlittableJsonReaderObject, LatestVersionCheck.VersionInfo> LatestVersionCheckVersionInfo = GenerateJsonDeserializationRoutine<LatestVersionCheck.VersionInfo>();

        public static readonly Func<BlittableJsonReaderObject, License> License = GenerateJsonDeserializationRoutine<License>();

        public static readonly Func<BlittableJsonReaderObject, UserRegistrationInfo> UserRegistrationInfo = GenerateJsonDeserializationRoutine<UserRegistrationInfo>();

        public static readonly Func<BlittableJsonReaderObject, FeedbackForm> FeedbackForm = GenerateJsonDeserializationRoutine<FeedbackForm>();
        public static readonly Func<BlittableJsonReaderObject, CustomIndexPaths> CustomIndexPaths = GenerateJsonDeserializationRoutine<CustomIndexPaths>();

        public static readonly Func<BlittableJsonReaderObject, CertificateDefinition> CertificateDefinition = GenerateJsonDeserializationRoutine<CertificateDefinition>();

        public static readonly Func<BlittableJsonReaderObject, RestoreSettings> RestoreSettings = GenerateJsonDeserializationRoutine<RestoreSettings>();

        public static readonly Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, IndexQueryServerSide> IndexQuery = GenerateJsonDeserializationRoutine<IndexQueryServerSide>();

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisQueryServerSide> MoreLikeThisQuery = GenerateJsonDeserializationRoutine<MoreLikeThisQueryServerSide>();

        public static readonly Func<BlittableJsonReaderObject, FacetQueryServerSide> FacetQuery = GenerateJsonDeserializationRoutine<FacetQueryServerSide>();

        public static readonly Func<BlittableJsonReaderObject, SuggestionQueryServerSide> SuggestionQuery = GenerateJsonDeserializationRoutine<SuggestionQueryServerSide>();
    }
}
