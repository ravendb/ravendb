using System.Net.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Remote
{
    /// <summary>
    /// Represents a maintenance operation to retrieve the current remote attachments configuration from RavenDB.
    /// This operation allows you to fetch the settings for automatic upload  of attachments to remote storage destinations.
    /// </summary>
    public sealed class GetRemoteAttachmentsConfigurationOperation : IMaintenanceOperation<RemoteAttachmentsConfiguration>
    {
        /// <summary>
        /// Gets the command that will be executed to retrieve the remote attachments configuration.
        /// </summary>
        /// <param name="conventions">The document conventions to use for the request.</param>
        /// <param name="context">The JSON operation context for deserialization.</param>
        /// <returns>A <see cref="RavenCommand{T}"/> that retrieves the remote attachments configuration.</returns>
        public RavenCommand<RemoteAttachmentsConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRemoteAttachmentsConfigurationCommand();
        }

        internal sealed class GetRemoteAttachmentsConfigurationCommand : RavenCommand<RemoteAttachmentsConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/attachments/remote/config";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.RemoteAttachmentsConfiguration(response);
            }
        }
    }
}
