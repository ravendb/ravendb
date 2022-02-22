using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexPerformanceStatisticsOperation : IMaintenanceOperation<IndexPerformanceStats[]>
    {
        private readonly int? _shard;
        private readonly string[] _indexNames;

        public GetIndexPerformanceStatisticsOperation()
        {
        }

        internal GetIndexPerformanceStatisticsOperation(int? shard)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Client API");
            _shard = shard;
        }

        public GetIndexPerformanceStatisticsOperation(string[] indexNames)
        {
            _indexNames = indexNames ?? throw new ArgumentNullException(nameof(indexNames));
        }

        internal GetIndexPerformanceStatisticsOperation(string[] indexNames, int shard)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Client API");
            _indexNames = indexNames ?? throw new ArgumentNullException(nameof(indexNames));
            _shard = shard;
        }

        public RavenCommand<IndexPerformanceStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexPerformanceStatisticsCommand(_indexNames, _shard);
        }

        internal class GetIndexPerformanceStatisticsCommand : RavenCommand<IndexPerformanceStats[]>
        {
            private readonly string[] _indexNames;
            private readonly int? _shard;

            public GetIndexPerformanceStatisticsCommand(string[] indexNames, int? shard)
            {
                _indexNames = indexNames;
                _shard = shard;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = GetUrl(node);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Results", out BlittableJsonReaderArray results) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var stats = new IndexPerformanceStats[results.Length];
                for (var i = 0; i < results.Length; i++)
                {
                    stats[i] = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<IndexPerformanceStats>((BlittableJsonReaderObject)results[i]);
                }

                Result = stats;
            }

            private string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/indexes/performance";

                var first = true;
                if (_indexNames != null)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                    {
                        url += $"&name={Uri.EscapeDataString(indexName)}";
                    }
                    first = false;
                }

                if (_shard != null)
                {
                    if (first)
                        url += "?";
                    url += $"&shard={_shard}";
                }

                return url;
            }

            public override bool IsReadRequest => true;
        }
    }
}
