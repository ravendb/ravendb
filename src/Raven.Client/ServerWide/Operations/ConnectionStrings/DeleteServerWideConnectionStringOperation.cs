using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public sealed class RemoveServerWideConnectionStringOperation<T> : IServerOperation<RemoveServerWideConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;

        public RemoveServerWideConnectionStringOperation(T connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public RavenCommand<RemoveServerWideConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RemoveServerWideConnectionStringCommand(_connectionString);
        }

        private sealed class RemoveServerWideConnectionStringCommand : RavenCommand<RemoveServerWideConnectionStringResult>, IRaftCommand
        {
            private readonly T _connectionString;

            public RemoveServerWideConnectionStringCommand(T connectionString)
            {
                _connectionString = connectionString;
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings?connectionString={Uri.EscapeDataString(_connectionString.Name)}&type={_connectionString.Type}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.RemoveServerWideConnectionStringResult(response);
            }
        }
    }

    public sealed class RemoveServerWideConnectionStringResult
    {
        public long RaftCommandIndex { get; set; }
    }
}
