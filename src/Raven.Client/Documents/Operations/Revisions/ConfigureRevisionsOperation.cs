using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
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

        private class ConfigureRevisionsCommand : RavenCommand<ConfigureRevisionsOperationResult>, IRaftCommand
        {
            private readonly RevisionsConfiguration _configuration;

            public ConfigureRevisionsCommand(RevisionsConfiguration configuration)
            {
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
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
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx);
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

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public class ConfigureRevisionsOperationResult
    {
        public long? RaftCommandIndex { get; set; }
    }
}
