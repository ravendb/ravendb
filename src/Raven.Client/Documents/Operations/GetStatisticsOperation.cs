using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetStatisticsOperation : IMaintenanceOperation<DatabaseStatistics>
    {
        private readonly string _debugTag;
        private readonly string _nodeTag;

        public GetStatisticsOperation()
        {
        }

        internal GetStatisticsOperation(string debugTag, string nodeTag = null)
        {
            _debugTag = debugTag;
            _nodeTag = nodeTag;
        }

        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand(_debugTag, _nodeTag);
        }

        private class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
        {
            private readonly string _debugTag;

            public GetStatisticsCommand(string debugTag, string nodeTag)
            {
                _debugTag = debugTag;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/stats";
                if (_debugTag != null)
                    url += "?" + _debugTag;

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.GetStatisticsResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
