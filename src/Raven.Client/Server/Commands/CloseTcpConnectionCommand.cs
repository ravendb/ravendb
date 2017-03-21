using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class CloseTcpConnectionCommand : RavenCommand<object>
    {
        private readonly long _id;

        public CloseTcpConnectionCommand(long id)
        {
            _id = id;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/tcp?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
        }
    }
}