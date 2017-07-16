using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ConnectionStrings
{
    public class UpdateConnectionStringOperation<T> : IServerOperation<UpdateConnectionStringResult> where T : ConnectionString
    {
        private readonly T _connectionString;
        private readonly string _oldConnectionStringName;
        private readonly string _databaseName;

        public UpdateConnectionStringOperation(T connectionString, string oldConnectionStringName,  string databaseName)
        {
            _connectionString = connectionString;
            _oldConnectionStringName = oldConnectionStringName;
            _databaseName = databaseName;
        }

        public RavenCommand<UpdateConnectionStringResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateConnectionStringCommand(_connectionString, _databaseName, _oldConnectionStringName, context);
        }

        public class UpdateConnectionStringCommand : RavenCommand<UpdateConnectionStringResult>
        {
            private readonly T _connectionString;
            private readonly string _oldConnectionStringName;
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;

            public UpdateConnectionStringCommand(T connectionString, string databaseName, string oldConnectionStringName, JsonOperationContext context)
            {
                _connectionString = connectionString;
                _oldConnectionStringName = oldConnectionStringName;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/connection-strings/update?name={_databaseName}&oldName={_oldConnectionStringName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var connectionStringBlittable = EntityToBlittable.ConvertEntityToBlittable(_connectionString, DocumentConventions.Default, _context);
                        _context.Write(stream, connectionStringBlittable);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.UpdateConnectionStringResult(response);
            }
        }
    }

    public class UpdateConnectionStringResult
    {
        public long? ETag { get; set; }
    }
}