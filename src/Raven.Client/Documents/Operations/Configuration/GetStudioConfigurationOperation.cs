using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Configuration
{
    public class GetStudioConfigurationOperation : IMaintenanceOperation<StudioConfiguration>
    {
        public RavenCommand<StudioConfiguration> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStudioConfigurationCommand();
        }

        internal class GetStudioConfigurationCommand : RavenCommand<StudioConfiguration>
        {
            public override bool IsReadRequest => false;


            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/configuration/studio";

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

                Result = JsonDeserializationClient.StudioConfiguration(response);
            }
        }
    }
}
