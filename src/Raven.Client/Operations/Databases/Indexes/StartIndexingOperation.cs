using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class StartIndexingOperation : IAdminOperation
    {
        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new StartIndexingCommand();
        }

        private class StartIndexingCommand : RavenCommand<object>
        {
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/start";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}