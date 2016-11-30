using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class PutIndexCommand : RavenCommand<PutIndexResult>
    {
        public BlittableJsonReaderObject IndexDefinition;
        
        public JsonOperationContext Context;

        public string IndexName;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/indexes?name=" + Uri.EscapeUriString(IndexName);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, IndexDefinition);
                })
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.PutIndexResult(response);
        }
    }
}