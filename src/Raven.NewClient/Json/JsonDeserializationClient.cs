using System;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<BlittableJsonReaderObject, BatchResult> BatchResult = GenerateJsonDeserializationRoutine<BatchResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, QueryResult> QueryResult = GenerateJsonDeserializationRoutine<QueryResult>();

        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static readonly Func<BlittableJsonReaderObject, CreateDatabaseResult> CreateDatabaseResult = GenerateJsonDeserializationRoutine<CreateDatabaseResult>();

        public static readonly Func<BlittableJsonReaderObject, PutIndexResult> PutIndexResult = GenerateJsonDeserializationRoutine<PutIndexResult>();

        public static readonly Func<BlittableJsonReaderObject, GetIndexResult> GetIndexResult = GenerateJsonDeserializationRoutine<GetIndexResult>();

        public static readonly Func<BlittableJsonReaderObject, PutTransformerResult> PutTransformerResult = GenerateJsonDeserializationRoutine<PutTransformerResult>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseStatistics> GetStatisticsResult = GenerateJsonDeserializationRoutine<DatabaseStatistics>();

        public static readonly Func<BlittableJsonReaderObject, PatchResult> PatchResult = GenerateJsonDeserializationRoutine<PatchResult>();

        public static readonly Func<BlittableJsonReaderObject, DeleteResult> DeleteResult = GenerateJsonDeserializationRoutine<DeleteResult>();
		
		public static readonly Func<BlittableJsonReaderObject, HiLoResult> HiLoResult = GenerateJsonDeserializationRoutine<HiLoResult>();
    }
}