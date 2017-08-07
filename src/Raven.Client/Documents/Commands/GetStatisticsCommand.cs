using System.Net.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
    {
        public readonly string DebugTag;

        public GetStatisticsCommand()
        {
            
        }
        
        public GetStatisticsCommand(string debugTag)
        {
            DebugTag = debugTag;
        }
        
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stats";
            if (DebugTag != null)
                url += "?" + DebugTag;
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetStatisticsResult(response);
        }

        public override bool IsReadRequest => true;
    }
}