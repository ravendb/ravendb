using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations.Indexes
{
    public sealed class GetIndexesStatisticsOperation : IMaintenanceOperation<IndexStats[]>
    {
        public GetIndexesStatisticsOperation()
        {
            
        }

        public RavenCommand<IndexStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesStatisticsCommand();
        }

        internal sealed class GetIndexesStatisticsCommand : RavenCommand<IndexStats[]>
        {
            internal const string IncludeStatsParamName = "includeStats";
            private bool? _includeStats;
            public GetIndexesStatisticsCommand(string nodeTag = null, bool? includeStats = null)
            {
                _includeStats = includeStats;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats";

                if (_includeStats.HasValue)
                {
                    url += $"?{IncludeStatsParamName}={_includeStats}";
                }

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
