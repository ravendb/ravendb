using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class KillOperationCommand : RavenCommand
    {
        private readonly long _id;

        public KillOperationCommand(long id)
        {
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/kill?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}