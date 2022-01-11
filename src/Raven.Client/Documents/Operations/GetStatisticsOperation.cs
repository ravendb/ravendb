using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetStatisticsOperation : IMaintenanceOperation<DatabaseStatistics>
    {
        private readonly int? _shard;
        private readonly string _debugTag;
        private readonly string _nodeTag;

        public GetStatisticsOperation()
        {
        }

        public GetStatisticsOperation(int shard)
        {
            _shard = shard;
        }

        internal GetStatisticsOperation(string debugTag, string nodeTag = null, int? shard = null)
        {
            _debugTag = debugTag;
            _nodeTag = nodeTag;
            _shard = shard;
        }

        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand(_debugTag, _nodeTag, _shard);
        }

        internal class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
        {
            private readonly string _debugTag;
            private readonly int? _shard;

            public GetStatisticsCommand(string debugTag, string nodeTag, int? shard)
            {
                _debugTag = debugTag;
                _shard = shard;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/stats";
                var hasQueryString = false;
                if (_debugTag != null)
                {
                    url += "?" + _debugTag;
                    hasQueryString = true;
                }

                if (_shard.HasValue)
                {
                    if (hasQueryString == false)
                        url += "?";
                    url += $"&shard={_shard}";
                }

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
