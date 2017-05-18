using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    class GetNextOperationIdCommand : RavenCommand<long>
    {
        public override bool IsReadRequest => false; // disable caching
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/next-operation-id";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            long id;
            response.TryGet("Id", out id);
            Result = id;
        }
    }
}
