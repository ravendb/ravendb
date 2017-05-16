using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class StopIndexingOperation : IAdminOperation
    {
        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StopIndexingCommand();
        }

        private class StopIndexingCommand : RavenCommand<object>
        {
            public StopIndexingCommand()
            {
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/stop";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}