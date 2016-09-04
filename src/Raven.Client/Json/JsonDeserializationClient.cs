using System;
using Raven.Abstractions.Data;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();
    }
}