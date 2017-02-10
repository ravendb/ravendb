using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Data.Indexes;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class GetIndexingStatusOperation : IAdminOperation<IndexingStatus>
    {
        public RavenCommand<IndexingStatus> GetCommand(DocumentConventions conventions, JsonOperationContext context)
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