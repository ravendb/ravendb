using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Server.ETL;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ETL
{
    public class ToggleEtlStateOperation : IServerOperation<ToggleEtlStateOperationResult>
    {
        private readonly long _id;
        private readonly EtlType _type;
        private readonly string _databaseName;

        public ToggleEtlStateOperation(long id, EtlType type, string databaseName)
        {
            _id = id;
            _type = type;
            _databaseName = databaseName;
        }

        public RavenCommand<ToggleEtlStateOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleEtlStateCommand(_id, _type, _databaseName, context);
        }

        public class ToggleEtlStateCommand : RavenCommand<ToggleEtlStateOperationResult>
        {
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;
            private readonly long _id;
            private readonly EtlType _type;

            public ToggleEtlStateCommand(long id, EtlType type, string databaseName, JsonOperationContext context)
            {
                _id = id;
                _type = type;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/toggleState?id={_id}&name={_databaseName}&type={_type}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new StringContent("{}") // TODO arek
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ToggleEtlStateOperationResult(response);
            }
        }
    }

    public class ToggleEtlStateOperationResult
    {
        public long? ETag { get; set; }
    }
}