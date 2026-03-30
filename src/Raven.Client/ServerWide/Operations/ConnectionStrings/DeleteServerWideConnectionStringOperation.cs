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
    public sealed class DeleteServerWideConnectionStringOperation : IServerOperation<DeleteServerWideConnectionStringResult>
    {
        private readonly string _connectionStringName;
        private readonly ConnectionStringType _type;

        public DeleteServerWideConnectionStringOperation(string connectionStringName, ConnectionStringType type)
        {
            _connectionStringName = connectionStringName ?? throw new ArgumentNullException(nameof(connectionStringName));
            _type = type;
        }

        public RavenCommand<DeleteServerWideConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteServerWideConnectionStringCommand(_connectionStringName, _type);
        }

        private sealed class DeleteServerWideConnectionStringCommand : RavenCommand<DeleteServerWideConnectionStringResult>, IRaftCommand
        {
            private readonly string _connectionStringName;
            private readonly ConnectionStringType _type;

            public DeleteServerWideConnectionStringCommand(string connectionStringName, ConnectionStringType type)
            {
                _connectionStringName = connectionStringName;
                _type = type;
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings?name={Uri.EscapeDataString(_connectionStringName)}&type={_type}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DeleteServerWideConnectionStringResult(response);
            }
        }
    }

    public sealed class DeleteServerWideConnectionStringResult
    {
        public long RaftCommandIndex { get; set; }
    }
}
