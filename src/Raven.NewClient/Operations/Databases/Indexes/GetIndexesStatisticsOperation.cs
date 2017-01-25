using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Data.Indexes;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class GetIndexesStatisticsOperation : IAdminOperation<IndexStats[]>
    {
        public RavenCommand<IndexStats[]> GetCommand(DocumentConvention conventions, JsonOperationContext context)
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