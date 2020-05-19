using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetDetailedStatisticsOperation : IMaintenanceOperation<DetailedDatabaseStatistics>
    {
        private readonly string _debugTag;

        public GetDetailedStatisticsOperation()
        {
        }

        internal GetDetailedStatisticsOperation(string debugTag)
        {
            _debugTag = debugTag;
        }

        public RavenCommand<DetailedDatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DetailedDatabaseStatisticsCommand(_debugTag);
        }

        private class DetailedDatabaseStatisticsCommand : RavenCommand<DetailedDatabaseStatistics>
        {
            private readonly string _debugTag;

            public DetailedDatabaseStatisticsCommand(string debugTag)
            {
                _debugTag = debugTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/stats/detailed";
                if (_debugTag != null)
                    url += "?" + _debugTag;

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.GetDetailedStatisticsResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
