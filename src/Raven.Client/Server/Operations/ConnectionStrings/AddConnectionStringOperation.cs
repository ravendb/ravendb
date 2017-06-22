using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ConnectionStrings
{
    public class AddConnectionStringOperation<T> : IServerOperation<AddConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;
        private readonly string _databaseName;

        public AddConnectionStringOperation(T connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
        }

        public RavenCommand<AddConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddConnectionStringCommand(_connectionString, _databaseName, context);
        }

        public class AddConnectionStringCommand : RavenCommand<AddConnectionStringResult>
        {
            private readonly T _connectionString;
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;

            public AddConnectionStringCommand(T connectionString, string databaseName, JsonOperationContext context)
            {
                _connectionString = connectionString;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/connection-strings/add?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_connectionString, DocumentConventions.Default, _context);
                        _context.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.AddConnectionStringResult(response);
            }
        }
    }

    public class AddConnectionStringResult
    {
        public long? ETag { get; set; }
    }
}