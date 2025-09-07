using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    /// <summary>
    /// Represents a maintenance operation to retrieve the current retired attachments configuration from RavenDB.
    /// This operation allows you to fetch the settings for automatic retirement of attachments to external storage destinations.
    /// </summary>
    public sealed class GetRetireAttachmentsConfigurationOperation : IMaintenanceOperation<RetiredAttachmentsConfiguration>
    {
        /// <summary>
        /// Gets the command that will be executed to retrieve the retired attachments configuration.
        /// </summary>
        /// <param name="conventions">The document conventions to use for the request.</param>
        /// <param name="context">The JSON operation context for deserialization.</param>
        /// <returns>A <see cref="RavenCommand{T}"/> that retrieves the retired attachments configuration.</returns>
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
