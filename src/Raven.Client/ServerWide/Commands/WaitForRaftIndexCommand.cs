using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class WaitForRaftIndexCommand : RavenCommand
    {
        private readonly long _index;

        public WaitForRaftIndexCommand(long index)
        {
            _index = index;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/rachis/waitfor?index={_index}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
        }

        public override bool IsReadRequest => true;
    }
}
