using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Expiration
{
    internal class GetDocumentsExpirationConfigurationOperation : IMaintenanceOperation<ExpirationConfiguration>
    {
        public RavenCommand<ExpirationConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDocumentsExpirationConfigurationCommand();
        }

        internal class GetDocumentsExpirationConfigurationCommand : RavenCommand<ExpirationConfiguration>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/expiration/config";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationServer.ExpirationConfiguration(response);
            }
        }
    }
}
