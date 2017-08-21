using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class CloseTcpConnectionCommand : RavenCommand
    {
        private readonly long _id;

        public CloseTcpConnectionCommand(long id)
        {
            _id = id;
        }

        
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/tcp?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }
    }
}
