using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Http.OAuth;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Operations;
using Raven.Client.Server.Tcp;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonDeserializationClient : JsonDeserializationBase
    {        
        public static readonly Func<BlittableJsonReaderObject, IsDatabaseLoadedCommand.CommandResult> IsDatabaseLoadedCommandResult = GenerateJsonDeserializationRoutine<IsDatabaseLoadedCommand.CommandResult>();

        public static readonly Func<BlittableJsonReaderObject, GetConflictsResult.Conflict> DocumentConflict = GenerateJsonDeserializationRoutine<GetConflictsResult.Conflict>();

        public static readonly Func<BlittableJsonReaderObject, GetConflictsResult> GetConflictsResult = GenerateJsonDeserializationRoutine<GetConflictsResult>();

        public static readonly Func<BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, AttachmentResult> AttachmentResult = GenerateJsonDeserializationRoutine<AttachmentResult>();

        public static readonly Func<BlittableJsonReaderObject, QueryResult> QueryResult = GenerateJsonDeserializationRoutine<QueryResult>();

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisQueryResult> MoreLikeThisQueryResult = GenerateJsonDeserializationRoutine<MoreLikeThisQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> Topology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, ClusterTopologyResponse> ClusterTopology = GenerateJsonDeserializationRoutine<ClusterTopologyResponse>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<BlittableJsonReaderObject, CreateDatabaseResult> CreateDatabaseResult = GenerateJsonDeserializationRoutine<CreateDatabaseResult>();
        public static readonly Func<BlittableJsonReaderObject, ModifyDatabaseWatchersResult> ModifyDatabaseWatchersResult = GenerateJsonDeserializationRoutine<ModifyDatabaseWatchersResult>();
        public static readonly Func<BlittableJsonReaderObject, ModifySolverResult> ModifySolverResult = GenerateJsonDeserializationRoutine<ModifySolverResult>();

        public static readonly Func<BlittableJsonReaderObject, DisableDatabaseToggleResult> DisableResourceToggleResult = GenerateJsonDeserializationRoutine<DisableDatabaseToggleResult>();

        public static readonly Func<BlittableJsonReaderObject, BlittableArrayResult> BlittableArrayResult = GenerateJsonDeserializationRoutine<BlittableArrayResult>();

        public static readonly Func<BlittableJsonReaderObject, PutTransformerResult> PutTransformerResult = GenerateJsonDeserializationRoutine<PutTransformerResult>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatistics> GetStatisticsResult = GenerateJsonDeserializationRoutine<DatabaseStatistics>();

        public static readonly Func<BlittableJsonReaderObject, OperationIdResult> OperationIdResult = GenerateJsonDeserializationRoutine<OperationIdResult>();

        public static readonly Func<BlittableJsonReaderObject, HiLoResult> HiLoResult = GenerateJsonDeserializationRoutine<HiLoResult>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo = GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionServerMessage> SubscriptionNextObjectResult = GenerateJsonDeserializationRoutine<SubscriptionConnectionServerMessage>();

        public static readonly Func<BlittableJsonReaderObject, CreateSubscriptionResult> CreateSubscriptionResult = GenerateJsonDeserializationRoutine<CreateSubscriptionResult>();

        public static readonly Func<BlittableJsonReaderObject, GetSubscriptionsResult> GetSubscriptionsResult = GenerateJsonDeserializationRoutine<GetSubscriptionsResult>();

        public static readonly Func<BlittableJsonReaderObject, FacetedQueryResult> FacetedQueryResult = GenerateJsonDeserializationRoutine<FacetedQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, TermsQueryResult> TermsQueryResult = GenerateJsonDeserializationRoutine<TermsQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, IndexingStatus> IndexingStatus = GenerateJsonDeserializationRoutine<IndexingStatus>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexesResponse> GetIndexesResponse = GenerateJsonDeserializationRoutine<GetIndexesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetTransformersResponse> GetTransformersResponse = GenerateJsonDeserializationRoutine<GetTransformersResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexNamesResponse> GetIndexNamesResponse = GenerateJsonDeserializationRoutine<GetIndexNamesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetTransformerNamesResponse> GetTransformerNamesResponse = GenerateJsonDeserializationRoutine<GetTransformerNamesResponse>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexStatisticsResponse> GetIndexStatisticsResponse = GenerateJsonDeserializationRoutine<GetIndexStatisticsResponse>();

        public static readonly Func<BlittableJsonReaderObject, PutIndexesResponse> PutIndexesResponse = GenerateJsonDeserializationRoutine<PutIndexesResponse>();

        public static readonly Func<BlittableJsonReaderObject, IndexErrors> IndexErrors = GenerateJsonDeserializationRoutine<IndexErrors>();

        public static readonly Func<BlittableJsonReaderObject, PatchResult> PatchResult = GenerateJsonDeserializationRoutine<PatchResult>();

        public static readonly Func<BlittableJsonReaderObject, GetApiKeysResponse> GetApiKeysResponse = GenerateJsonDeserializationRoutine<GetApiKeysResponse>();

        public static readonly Func<BlittableJsonReaderObject, LiveTopologyInfo> LiveTopologyInfo = GenerateJsonDeserializationRoutine<LiveTopologyInfo>();

        public static readonly Func<BlittableJsonReaderObject, BuildNumber> BuildNumber = GenerateJsonDeserializationRoutine<BuildNumber>();

        internal static readonly Func<BlittableJsonReaderObject, ExceptionDispatcher.ExceptionSchema> ExceptionSchema = GenerateJsonDeserializationRoutine<ExceptionDispatcher.ExceptionSchema>();

        internal static readonly Func<BlittableJsonReaderObject, DeleteDatabaseResult> DeleteDatabaseResult = GenerateJsonDeserializationRoutine<DeleteDatabaseResult>();

        internal static readonly Func<BlittableJsonReaderObject, ConfigureExpirationOperationResult> ConfigureExpirationOperationResult = GenerateJsonDeserializationRoutine<ConfigureExpirationOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, ConfigurePeriodicBackupOperationResult> ConfigurePeriodicExportBundleOperationResult = GenerateJsonDeserializationRoutine<ConfigurePeriodicBackupOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, GetPeriodicBackupStatusOperationResult> GetPeriodicBackupStatusOperationResult = GenerateJsonDeserializationRoutine<GetPeriodicBackupStatusOperationResult>();

        internal static readonly Func<BlittableJsonReaderObject, ConfigureVersioningOperationResult> ConfigureVersioningOperationResult = GenerateJsonDeserializationRoutine<ConfigureVersioningOperationResult>();
        internal static readonly Func<BlittableJsonReaderObject, SubscriptionState> SubscriptionState = GenerateJsonDeserializationRoutine<SubscriptionState>();

        internal static readonly Func<BlittableJsonReaderObject, DatabaseWatcher> DatabaseWatcher= GenerateJsonDeserializationRoutine<DatabaseWatcher>();
    }
}