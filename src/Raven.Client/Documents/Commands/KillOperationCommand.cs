using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class KillOperationCommand : RavenCommand
    {
        private readonly long _id;

        public KillOperationCommand(long id)
        {
            _id = id;
        }

        public KillOperationCommand(long id, string nodeTag) : this(id)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/kill?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
