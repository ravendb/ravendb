using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Operations
{
    public class KillServerOperationCommand : RavenCommand
    {
        private readonly long _id;

        public KillServerOperationCommand(long id)
        {
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/operations/kill?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
