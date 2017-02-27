using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetIndexCommand : RavenCommand<BlittableArrayResult>
    {
        public JsonOperationContext Context;

        public string IndexName;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            // If index name is not supplied, then we request to get all index definitions
            // TODO iftah, should we also specify pageSize and start?
            url = $"{node.Url}/databases/{node.Database}/indexes?" + (IndexName != null ? "name=" + Uri.EscapeDataString(IndexName) : string.Empty);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}