using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, SmugglerProgressBase.CountsWithSkippedCountAndLastEtag> CountsWithSkippedCountAndLastEtag = GenerateJsonDeserializationRoutine<SmugglerProgressBase.CountsWithSkippedCountAndLastEtag>();

        public static readonly Func<BlittableJsonReaderObject, IsDatabaseLoadedCommand.CommandResult> IsDatabaseLoadedCommandResult = GenerateJsonDeserializationRoutine<IsDatabaseLoadedCommand.CommandResult>();

        public static readonly Func<BlittableJsonReaderObject, GetConflictsResult.Conflict> DocumentConflict = GenerateJsonDeserializationRoutine<GetConflictsResult.Conflict>();

        public static readonly Func<BlittableJsonReaderObject, GetConflictsResult> GetConflictsResult = GenerateJsonDeserializationRoutine<GetConflictsResult>();

        public static readonly Func<BlittableJsonReaderObject, GetDocumentsResult> GetDocumentsResult = GenerateJsonDeserializationRoutine<GetDocumentsResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, AttachmentDetails> AttachmentDetails = GenerateJsonDeserializationRoutine<AttachmentDetails>();

        public static readonly Func<BlittableJsonReaderObject, AttachmentName> AttachmentName = GenerateJsonDeserializationRoutine<AttachmentName>();

        public static readonly Func<BlittableJsonReaderObject, QueryResult> QueryResult = GenerateJsonDeserializationRoutine<QueryResult>();

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisQueryResult> MoreLikeThisQueryResult = GenerateJsonDeserializationRoutine<MoreLikeThisQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, Topology> Topology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTopologyResponse> ClusterTopology = GenerateJsonDeserializationRoutine<ClusterTopologyResponse>();

        public static readonly Func<BlittableJsonReaderObject, NodeInfo> NodeInfo = GenerateJsonDeserializationRoutine<NodeInfo>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, DatabasePutResult> DatabasePutResult = GenerateJsonDeserializationRoutine<DatabasePutResult>();

        public static readonly Func<BlittableJsonReaderObject, GetLogsConfigurationResult> GetLogsConfigurationResult = GenerateJsonDeserializationRoutine<GetLogsConfigurationResult>();

        public static readonly Func<BlittableJsonReaderObject, ModifyOngoingTaskResult> ModifyOngoingTaskResult = GenerateJsonDeserializationRoutine<ModifyOngoingTaskResult>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskSubscription> GetOngoingTaskSubscriptionResult = GenerateJsonDeserializationRoutine<OngoingTaskSubscription>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskPullReplicationAsSink> OngoingTaskPullReplicationAsSinkResult = GenerateJsonDeserializationRoutine<OngoingTaskPullReplicationAsSink>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskPullReplicationAsHub> OngoingTaskPullReplicationAsHubResult = GenerateJsonDeserializationRoutine<OngoingTaskPullReplicationAsHub>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskReplication> GetOngoingTaskReplicationResult = GenerateJsonDeserializationRoutine<OngoingTaskReplication>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskRavenEtlDetails> GetOngoingTaskRavenEtlResult = GenerateJsonDeserializationRoutine<OngoingTaskRavenEtlDetails>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskBackup> GetOngoingTaskBackupResult = GenerateJsonDeserializationRoutine<OngoingTaskBackup>();

        public static readonly Func<BlittableJsonReaderObject, OngoingTaskSqlEtlDetails> GetOngoingTaskSqlEtlResult = GenerateJsonDeserializationRoutine<OngoingTaskSqlEtlDetails>();

        public static readonly Func<BlittableJsonReaderObject, ModifySolverResult> ModifySolverResult = GenerateJsonDeserializationRoutine<ModifySolverResult>();

        public static readonly Func<BlittableJsonReaderObject, DisableDatabaseToggleResult> DisableResourceToggleResult = GenerateJsonDeserializationRoutine<DisableDatabaseToggleResult>();

        public static readonly Func<BlittableJsonReaderObject, BlittableArrayResult> BlittableArrayResult = GenerateJsonDeserializationRoutine<BlittableArrayResult>();

        public static readonly Func<BlittableJsonReaderObject, BatchCommandResult> BatchCommandResult = GenerateJsonDeserializationRoutine<BatchCommandResult>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatistics> GetStatisticsResult = GenerateJsonDeserializationRoutine<DatabaseStatistics>();

        public static readonly Func<BlittableJsonReaderObject, DetailedDatabaseStatistics> GetDetailedStatisticsResult = GenerateJsonDeserializationRoutine<DetailedDatabaseStatistics>();

        public static readonly Func<BlittableJsonReaderObject, OperationIdResult> OperationIdResult = GenerateJsonDeserializationRoutine<OperationIdResult>();

        public static readonly Func<BlittableJsonReaderObject, ReplayTxOperationResult> GetReplayTrxOperationResult = GenerateJsonDeserializationRoutine<ReplayTxOperationResult>();

        public static readonly Func<BlittableJsonReaderObject, HiLoResult> HiLoResult = GenerateJsonDeserializationRoutine<HiLoResult>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo = GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionServerMessage> SubscriptionNextObjectResult = GenerateJsonDeserializationRoutine<SubscriptionConnectionServerMessage>();

        public static readonly Func<BlittableJsonReaderObject, CreateSubscriptionResult> CreateSubscriptionResult = GenerateJsonDeserializationRoutine<CreateSubscriptionResult>();

        public static readonly Func<BlittableJsonReaderObject, GetSubscriptionsResult> GetSubscriptionsResult = GenerateJsonDeserializationRoutine<GetSubscriptionsResult>();

        public static readonly Func<BlittableJsonReaderObject, TermsQueryResult> TermsQueryResult = GenerateJsonDeserializationRoutine<TermsQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, IndexingStatus> IndexingStatus = GenerateJsonDeserializationRoutine<IndexingStatus>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexesResponse> GetIndexesResponse = GenerateJsonDeserializationRoutine<GetIndexesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexNamesResponse> GetIndexNamesResponse = GenerateJsonDeserializationRoutine<GetIndexNamesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexStatisticsResponse> GetIndexStatisticsResponse = GenerateJsonDeserializationRoutine<GetIndexStatisticsResponse>();

        public static readonly Func<BlittableJsonReaderObject, PutIndexesResponse> PutIndexesResponse = GenerateJsonDeserializationRoutine<PutIndexesResponse>();

        public static readonly Func<BlittableJsonReaderObject, IndexErrors> IndexErrors = GenerateJsonDeserializationRoutine<IndexErrors>();

        public static readonly Func<BlittableJsonReaderObject, PatchResult> PatchResult = GenerateJsonDeserializationRoutine<PatchResult>();

        public static readonly Func<BlittableJsonReaderObject, GetCertificatesResponse> GetCertificatesResponse = GenerateJsonDeserializationRoutine<GetCertificatesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetClientCertificatesResponse> GetClientCertificatesResponse = GenerateJsonDeserializationRoutine<GetClientCertificatesResponse>();

        public static readonly Func<BlittableJsonReaderObject, BuildNumber> BuildNumber = GenerateJsonDeserializationRoutine<BuildNumber>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionState> SubscriptionState = GenerateJsonDeserializationRoutine<SubscriptionState>();

        public static readonly Func<BlittableJsonReaderObject, CountersDetail> CountersDetail = GenerateJsonDeserializationRoutine<CountersDetail>();

        internal static readonly Func<BlittableJsonReaderObject, ExceptionDispatcher.ExceptionSchema> ExceptionSchema = GenerateJsonDeserializationRoutine<ExceptionDispatcher.ExceptionSchema>();

        internal static readonly Func<BlittableJsonReaderObject, DeleteDatabaseResult> DeleteDatabaseResult = GenerateJsonDeserializationRoutine<DeleteDatabaseResult>();

        internal static readonly Func<BlittableJsonReaderObject, ConfigureExpirationOperationResult> ConfigureExpirationOperationResult = GenerateJsonDeserializationRoutine<ConfigureExpirationOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, UpdatePeriodicBackupOperationResult> ConfigurePeriodicBackupOperationResult = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, StartBackupOperationResult> BackupDatabaseNowResult = GenerateJsonDeserializationRoutine<StartBackupOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, GetPeriodicBackupStatusOperationResult> GetPeriodicBackupStatusOperationResult = GenerateJsonDeserializationRoutine<GetPeriodicBackupStatusOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, PeriodicBackupStatus> PeriodicBackupStatus = GenerateJsonDeserializationRoutine<PeriodicBackupStatus>();

        internal static readonly Func<BlittableJsonReaderObject, ConfigureRevisionsOperationResult> ConfigureRevisionsOperationResult = GenerateJsonDeserializationRoutine<ConfigureRevisionsOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, ExternalReplication> ExternalReplication = GenerateJsonDeserializationRoutine<ExternalReplication>();

        internal static readonly Func<BlittableJsonReaderObject, PullReplicationAsSink> PullReplicationAsSink = GenerateJsonDeserializationRoutine<PullReplicationAsSink>();

        internal static readonly Func<BlittableJsonReaderObject, PullReplicationDefinition> PullReplicationDefinition = GenerateJsonDeserializationRoutine<PullReplicationDefinition>();

        internal static readonly Func<BlittableJsonReaderObject, AddEtlOperationResult> AddEtlOperationResult = GenerateJsonDeserializationRoutine<AddEtlOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, UpdateEtlOperationResult> UpdateEtlOperationResult = GenerateJsonDeserializationRoutine<UpdateEtlOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, EtlProcessState> EtlProcessState = GenerateJsonDeserializationRoutine<EtlProcessState>();

        internal static readonly Func<BlittableJsonReaderObject, PutConnectionStringResult> PutConnectionStringResult = GenerateJsonDeserializationRoutine<PutConnectionStringResult>();

        internal static readonly Func<BlittableJsonReaderObject, RemoveConnectionStringResult> RemoveConnectionStringResult = GenerateJsonDeserializationRoutine<RemoveConnectionStringResult>();

        internal static readonly Func<BlittableJsonReaderObject, GetConnectionStringsResult> GetConnectionStringsResult = GenerateJsonDeserializationRoutine<GetConnectionStringsResult>();

        internal static readonly Func<BlittableJsonReaderObject, SmugglerResult> SmugglerResult = GenerateJsonDeserializationRoutine<SmugglerResult>();

        internal static readonly Func<BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        internal static readonly Func<BlittableJsonReaderObject, StudioConfiguration> StudioConfiguration = GenerateJsonDeserializationRoutine<StudioConfiguration>();

        internal static readonly Func<BlittableJsonReaderObject, ServerWideStudioConfiguration> ServerWideStudioConfiguration = GenerateJsonDeserializationRoutine<ServerWideStudioConfiguration>();

        internal static readonly Func<BlittableJsonReaderObject, GetClientConfigurationOperation.Result> ClientConfigurationResult = GenerateJsonDeserializationRoutine<GetClientConfigurationOperation.Result>();

        internal static readonly Func<BlittableJsonReaderObject, S3Settings> S3Settings = GenerateJsonDeserializationRoutine<S3Settings>();

        internal static readonly Func<BlittableJsonReaderObject, GlacierSettings> GlacierSettings = GenerateJsonDeserializationRoutine<GlacierSettings>();

        internal static readonly Func<BlittableJsonReaderObject, AzureSettings> AzureSettings = GenerateJsonDeserializationRoutine<AzureSettings>();

        internal static readonly Func<BlittableJsonReaderObject, FtpSettings> FtpSettings = GenerateJsonDeserializationRoutine<FtpSettings>();

        internal static readonly Func<BlittableJsonReaderObject, ClaimDomainResult> ClaimDomainResult = GenerateJsonDeserializationRoutine<ClaimDomainResult>();

        internal static readonly Func<BlittableJsonReaderObject, ForceRenewResult> ForceRenewResult = GenerateJsonDeserializationRoutine<ForceRenewResult>();

        internal static readonly Func<BlittableJsonReaderObject, CounterBatch> CounterBatch = GenerateJsonDeserializationRoutine<CounterBatch>();
    }
}
