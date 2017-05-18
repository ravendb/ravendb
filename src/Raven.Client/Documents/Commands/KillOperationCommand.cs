using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    class KillOperationCommand : RavenCommand<bool>
    {
        private readonly long _id;

        public KillOperationCommand(long id)
        {
            _id = id;
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/kill?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = true;
        }

        public override bool IsReadRequest => false;
    
    }
}
