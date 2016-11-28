using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class CreateDatabaseCommand : RavenCommand<CreateDatabaseResult>
    {
        public BlittableJsonReaderObject DatabaseDocument;
        
        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/databases/{node.Database}";
            IsReadRequest = false;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, DatabaseDocument);
                })
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {

        }
    }
}