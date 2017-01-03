using System;
using System.Net.Http;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetIndexCommand : RavenCommand<BlittableArrayResult>
    {
        public JsonOperationContext Context;

        public string IndexName;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            // If index name is not supplied, then we request to get all index definitions
            // TODO iftah, should we also specify pageSize and start?
            url = $"{node.Url}/databases/{node.Database}/indexes?" + (IndexName != null ? "name=" + Uri.EscapeUriString(IndexName) : string.Empty);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }
    }
}