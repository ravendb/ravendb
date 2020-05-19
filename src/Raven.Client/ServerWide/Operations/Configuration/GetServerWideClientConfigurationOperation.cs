using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetServerWideClientConfigurationOperation : IServerOperation<ClientConfiguration>
    {
        public RavenCommand<ClientConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideClientConfigurationCommand();
        }

        private class GetServerWideClientConfigurationCommand : RavenCommand<ClientConfiguration>
        {
            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/configuration/client";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.ClientConfiguration(response);
            }
        }
    }
}
