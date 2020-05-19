using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexesStatisticsOperation : IMaintenanceOperation<IndexStats[]>
    {
        public RavenCommand<IndexStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesStatisticsCommand();
        }

        private class GetIndexesStatisticsCommand : RavenCommand<IndexStats[]>
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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