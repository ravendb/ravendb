using System.Net.Http;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
    {
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stats";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetStatisticsResult(response);
        }

        public override bool IsReadRequest => true;
    }
}