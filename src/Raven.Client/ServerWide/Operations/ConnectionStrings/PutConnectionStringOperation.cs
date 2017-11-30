using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public class PutConnectionStringOperation<T> : IServerOperation<PutConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;
        private readonly string _databaseName;

        public PutConnectionStringOperation(T connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
        }

        public RavenCommand<PutConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new PutConnectionStringCommand(_connectionString, _databaseName);
        }

        public class PutConnectionStringCommand : RavenCommand<PutConnectionStringResult>
        {
            private readonly T _connectionString;
            private readonly string _databaseName;

            public PutConnectionStringCommand(T connectionString, string databaseName)
            {
                _connectionString = connectionString;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/connection-strings";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_connectionString, DocumentConventions.Default, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.PutConnectionStringResult(response);
            }
        }
    }

    public class PutConnectionStringResult
    {
        public long? ETag { get; set; }
    }
}
