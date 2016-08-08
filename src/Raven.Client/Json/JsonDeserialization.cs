using System;
using Raven.Abstractions.Data;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        private static readonly Type[] EmptyTypes = new Type[0];

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();
        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();
    }
}