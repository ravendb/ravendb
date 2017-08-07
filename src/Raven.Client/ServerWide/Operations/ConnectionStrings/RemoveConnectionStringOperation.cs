using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ConnectionStrings
{
    public class RemoveConnectionStringOperation<T> : IServerOperation<RemoveConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;
        private readonly string _databaseName;

        public RemoveConnectionStringOperation(T connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
        }

        public RavenCommand<RemoveConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RemoveConnectionStringCommand(_connectionString, _databaseName);
        }

        public class RemoveConnectionStringCommand : RavenCommand<RemoveConnectionStringResult>
        {
            private readonly T _connectionString;
            private readonly string _databaseName;

            public RemoveConnectionStringCommand(T connectionString, string databaseName)
            {
                _connectionString = connectionString;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/connection-strings?name={_databaseName}&connectionString={_connectionString.Name}&type={_connectionString.Type}";

                var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Delete
                    };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.RemoveConnectionStringResult(response);
            }
        }
    }

    public class RemoveConnectionStringResult
    {
        public long? ETag { get; set; }
    }
}