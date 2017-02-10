using System.IO;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Data.Indexes;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class GetIndexingStatusOperation : IAdminOperation<IndexingStatus>
    {
        public RavenCommand<IndexingStatus> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetIndexingStatusCommand();
        }

        private class GetIndexingStatusCommand : RavenCommand<IndexingStatus>
        {
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/status";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.IndexingStatus(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}