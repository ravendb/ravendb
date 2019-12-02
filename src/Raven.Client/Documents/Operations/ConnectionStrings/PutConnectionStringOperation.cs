using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.ConnectionStrings
{
    public class PutConnectionStringOperation<T> : IMaintenanceOperation<PutConnectionStringResult> where T : ConnectionString
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

        private class PutConnectionStringCommand : RavenCommand<PutConnectionStringResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly T _connectionString;

            public PutConnectionStringCommand(DocumentConventions conventions, T connectionString)
            {
                _conventions = conventions;
                _connectionString = connectionString;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/connection-strings";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(this, stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_connectionString, ctx);
                        ctx.Write(stream, config);
                    })
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

    public class PutConnectionStringResult
    {
        [Obsolete("PutConnectionStringResult.ETag is not supported anymore. Will be removed in next major version of the product. Please use PutConnectionStringResult.RaftCommandIndex instead.")]
        public long? ETag { get; set; }

        public long RaftCommandIndex { get; set; }
    }
}
