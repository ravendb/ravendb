using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Data.Indexes;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class GetIndexesStatisticsOperation : IAdminOperation<IndexStats[]>
    {
        public RavenCommand<IndexStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesStatisticsCommand();
        }

        private class GetIndexesStatisticsCommand : RavenCommand<IndexStats[]>
        {
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var results = JsonDeserializationClient.GetIndexStatisticsResponse(response).Results;
                Result = results;
            }

            public override bool IsReadRequest => true;
        }
    }
}