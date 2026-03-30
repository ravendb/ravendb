using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public sealed class PutServerWideConnectionStringOperation : IServerOperation<PutServerWideConnectionStringResult>
    {
        private readonly ServerWideConnectionString _connectionString;

        public PutServerWideConnectionStringOperation(ServerWideConnectionString connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public RavenCommand<PutServerWideConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideConnectionStringCommand(conventions, context, _connectionString);
        }

        private sealed class PutServerWideConnectionStringCommand : RavenCommand<PutServerWideConnectionStringResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _connectionString;

            public PutServerWideConnectionStringCommand(DocumentConventions conventions, JsonOperationContext context, ServerWideConnectionString connectionString)
            {
                if (connectionString == null)
                    throw new ArgumentNullException(nameof(connectionString));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));

                _connectionString = context.ReadObject(connectionString.ToJson(), "server-wide-connection-string");
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/connection-strings";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _connectionString).ConfigureAwait(false), _conventions)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.PutServerWideConnectionStringResult(response);
            }
        }
    }

    public sealed class PutServerWideConnectionStringResult
    {
        public long RaftCommandIndex { get; set; }
    }
}
