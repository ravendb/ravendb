using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.ETL;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ETL
{
    public class DeleteEtlOperation : IServerOperation<DeleteEtlOperationResult>
    {
        private readonly long _id;
        private readonly EtlType _type;
        private readonly string _databaseName;

        public DeleteEtlOperation(long id, EtlType type, string databaseName)
        {
            _id = id;
            _type = type;
            _databaseName = databaseName;
        }

        public RavenCommand<DeleteEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteEtlCommand(_id, _type, _databaseName, context);
        }

        public class DeleteEtlCommand : RavenCommand<DeleteEtlOperationResult>
        {
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;
            private readonly long _id;
            private readonly EtlType _type;

            public DeleteEtlCommand(long id, EtlType type, string databaseName, JsonOperationContext context)
            {
                _id = id;
                _type = type;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/delete?id={_id}&name={_databaseName}&type={_type}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new StringContent("{}") // TODO arek
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DeleteEtlOperationResult(response);
            }
        }
    }

    public class DeleteEtlOperationResult
    {
        public long? ETag { get; set; }
    }
}