using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.ETL;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ETL
{
    public class ToggleEtlStateOperation : IServerOperation<ToggleEtlStateOperationResult>
    {
        private readonly string _configurationName;
        private readonly EtlType _type;
        private readonly string _databaseName;

        public ToggleEtlStateOperation(string configurationName, EtlType type, string databaseName)
        {
            _configurationName = configurationName;
            _type = type;
            _databaseName = databaseName;
        }

        public RavenCommand<ToggleEtlStateOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ToggleEtlStateCommand(_configurationName, _type, _databaseName, context);
        }

        public class ToggleEtlStateCommand : RavenCommand<ToggleEtlStateOperationResult>
        {
            private readonly string _databaseName;
            private readonly JsonOperationContext _context;
            private readonly string _configurationName;
            private readonly EtlType _type;

            public ToggleEtlStateCommand(string configurationName, EtlType type, string databaseName, JsonOperationContext context)
            {
                _configurationName = configurationName;
                _type = type;
                _databaseName = databaseName;
                _context = context;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/etl/toggleState?name={_databaseName}&type={_type}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(new { Name = _configurationName }, DocumentConventions.Default, _context);
                        _context.Write(stream, config);
                    })
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