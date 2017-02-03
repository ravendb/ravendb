using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Indexes;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Data.Transformers;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Replication.Messages;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Operations.Databases.Responses;
using Sparrow.Json;

namespace Raven.NewClient.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, QueryResult> QueryResult = GenerateJsonDeserializationRoutine<QueryResult>();

        public static readonly Func<BlittableJsonReaderObject, MoreLikeThisQueryResult> MoreLikeThisQueryResult = GenerateJsonDeserializationRoutine<MoreLikeThisQueryResult>();

        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static readonly Func<BlittableJsonReaderObject, CreateDatabaseResult> CreateDatabaseResult = GenerateJsonDeserializationRoutine<CreateDatabaseResult>();

        public static readonly Func<BlittableJsonReaderObject, PutIndexResult> PutIndexResult = GenerateJsonDeserializationRoutine<PutIndexResult>();

        public static readonly Func<BlittableJsonReaderObject, BlittableArrayResult> BlittableArrayResult = GenerateJsonDeserializationRoutine<BlittableArrayResult>();

        public static readonly Func<BlittableJsonReaderObject, PutTransformerResult> PutTransformerResult = GenerateJsonDeserializationRoutine<PutTransformerResult>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatistics> GetStatisticsResult = GenerateJsonDeserializationRoutine<DatabaseStatistics>();

        public static readonly Func<BlittableJsonReaderObject, OperationIdResult> OperationIdResult = GenerateJsonDeserializationRoutine<OperationIdResult>();

        public static readonly Func<BlittableJsonReaderObject, HiLoResult> HiLoResult = GenerateJsonDeserializationRoutine<HiLoResult>();

        public static readonly Func<BlittableJsonReaderObject, GetTcpInfoResult> GetTcpInfoResult = GenerateJsonDeserializationRoutine<GetTcpInfoResult>();

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

        public static readonly Func<BlittableJsonReaderObject, IndexErrors> IndexErrors = GenerateJsonDeserializationRoutine<IndexErrors>();

        public static readonly Func<BlittableJsonReaderObject, PatchResult> PatchResult = GenerateJsonDeserializationRoutine<PatchResult>();

        public static readonly Func<BlittableJsonReaderObject, GetApiKeysResponse> GetApiKeysResponse = GenerateJsonDeserializationRoutine<GetApiKeysResponse>();

        public static readonly Func<BlittableJsonReaderObject, FullTopologyInfo> FullTopologyInfo = GenerateJsonDeserializationRoutine<FullTopologyInfo>();

        public static readonly Func<BlittableJsonReaderObject, BuildNumber> BuildNumber = GenerateJsonDeserializationRoutine<BuildNumber>();

        internal static readonly Func<BlittableJsonReaderObject, ExceptionDispatcher.ExceptionSchema> ExceptionSchema = GenerateJsonDeserializationRoutine<ExceptionDispatcher.ExceptionSchema>();
    }
}