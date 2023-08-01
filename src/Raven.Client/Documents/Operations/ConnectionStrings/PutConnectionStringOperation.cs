using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public sealed class PutConnectionStringOperation<T> : IMaintenanceOperation<PutConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;

        public PutConnectionStringOperation(T connectionString)
        {
            _connectionString = connectionString;
        }

        public RavenCommand<PutConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new PutConnectionStringCommand(conventions, _connectionString);
        }

        private sealed class PutConnectionStringCommand : RavenCommand<PutConnectionStringResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly T _connectionString;

            public PutConnectionStringCommand(DocumentConventions conventions, T connectionString)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/connection-strings";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_connectionString, ctx)).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.PutConnectionStringResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public sealed class PutConnectionStringResult
    {
        public long RaftCommandIndex { get; set; }
    }
}
