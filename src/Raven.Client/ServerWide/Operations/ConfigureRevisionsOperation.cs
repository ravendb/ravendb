using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Revisions;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureRevisionsOperation : IMaintenanceOperation<ConfigureRevisionsOperationResult>
    {
        private readonly RevisionsConfiguration _configuration;

        public ConfigureRevisionsOperation(RevisionsConfiguration configuration)
        {
            _configuration = configuration;
        }
        public RavenCommand<ConfigureRevisionsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureRevisionsCommand(_configuration);
        }
    }

    public class ConfigureRevisionsCommand : RavenCommand<ConfigureRevisionsOperationResult>
    {
        private readonly RevisionsConfiguration _configuration;

        public ConfigureRevisionsCommand(RevisionsConfiguration configuration)
        {
            _configuration = configuration;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/revisions/config";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, ctx);
                    ctx.Write(stream, config);
                })
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.ConfigureRevisionsOperationResult(response);
        }
    }

    public class ConfigureRevisionsOperationResult
    {
        public long? ETag { get; set; }
    }
}
