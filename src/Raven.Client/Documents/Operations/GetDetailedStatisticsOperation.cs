using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public sealed class GetDetailedStatisticsOperation : IMaintenanceOperation<DetailedDatabaseStatistics>
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
            return new GetDetailedStatisticsCommand(_debugTag);
        }

        internal sealed class GetDetailedStatisticsCommand : RavenCommand<DetailedDatabaseStatistics>
        {
            private readonly string _debugTag;

            public GetDetailedStatisticsCommand(string debugTag)
                : this(debugTag, nodeTag: null)
            {
            }

            internal GetDetailedStatisticsCommand(string debugTag, string nodeTag)
            {
                _debugTag = debugTag;
                SelectedNodeTag = nodeTag;
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
