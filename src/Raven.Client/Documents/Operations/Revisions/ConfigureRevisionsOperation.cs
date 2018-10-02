using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
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

        private class ConfigureRevisionsCommand : RavenCommand<ConfigureRevisionsOperationResult>
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
                        var config = EntityToBlittable.ConvertCommandToBlittable(_configuration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureRevisionsOperationResult(response);
            }
        }
    }

    public class ConfigureRevisionsOperationResult
    {
        public long? RaftCommandIndex { get; set; }
    }
}
