using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    public class GetIdentitiesOperation : IMaintenanceOperation<Dictionary<string, long>>
    {
        public RavenCommand<Dictionary<string, long>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIdentitiesCommand();
        }

        private class GetIdentitiesCommand : RavenCommand<Dictionary<string, long>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/debug/identities";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = new Dictionary<string, long>();

                foreach (var propertyName in response.GetPropertyNames())
                {
                    Result[propertyName] = (long)response[propertyName];
                }
            }
        }
    }
}
