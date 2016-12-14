using System;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();
    }
}