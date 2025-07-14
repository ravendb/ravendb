using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class GetRetireAttachmentsConfigurationOperation : IMaintenanceOperation<RetiredAttachmentsConfiguration>
    {
        public RavenCommand<RetiredAttachmentsConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRetireAttachmentsConfigurationCommand();
        }

        internal sealed class GetRetireAttachmentsConfigurationCommand : RavenCommand<RetiredAttachmentsConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/attachments/retire/config";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.RetiredAttachmentsConfiguration(response);
            }
        }
    }
}
