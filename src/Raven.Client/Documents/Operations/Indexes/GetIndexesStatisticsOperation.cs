using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexesStatisticsOperation : IMaintenanceOperation<IndexStats[]>
    {
        private readonly int? _shard;

        public GetIndexesStatisticsOperation()
        {
            
        }

        internal GetIndexesStatisticsOperation(int? shard)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Client API");
            _shard = shard;
        }

        public RavenCommand<IndexStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesStatisticsCommand(_shard);
        }

        internal class GetIndexesStatisticsCommand : RavenCommand<IndexStats[]>
        {
            private readonly int? _shard;

            public GetIndexesStatisticsCommand(int? shard = null)
            {
                _shard = shard;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats";

                if (_shard != null)
                    url += $"?shard={_shard}";

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
