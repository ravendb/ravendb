using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class PutTransformerCommand : RavenCommand<PutTransformerResult>
    {
        public BlittableJsonReaderObject TransformerDefinition;
        
        public JsonOperationContext Context;

        public string TransformerName;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/transformers?name=" + Uri.EscapeUriString(TransformerName);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, TransformerDefinition);
                })
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.PutTransformerResult(response);
        }

        public override bool IsReadRequest => false;
    }
}