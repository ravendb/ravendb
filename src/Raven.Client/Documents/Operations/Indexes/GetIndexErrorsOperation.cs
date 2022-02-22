using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexErrorsOperation : IMaintenanceOperation<IndexErrors[]>
    {
        private readonly string[] _indexNames;
        private readonly int? _shard;
        private readonly string _nodeTag;

        public GetIndexErrorsOperation()
        {
        }

        internal GetIndexErrorsOperation(int? shard)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Client API");
            _shard = shard;
        }

        public GetIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        internal GetIndexErrorsOperation(string[] indexNames, int shard)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Client API");
            _indexNames = indexNames;
            _shard = shard;
        }

        internal GetIndexErrorsOperation(string[] indexNames, string nodeTag)
        {
            _indexNames = indexNames;
            _nodeTag = nodeTag;
        }

        public RavenCommand<IndexErrors[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexErrorsCommand(_indexNames, _nodeTag, _shard);
        }

        internal class GetIndexErrorsCommand : RavenCommand<IndexErrors[]>
        {
            private readonly string[] _indexNames;
            private readonly int? _shard;

            internal GetIndexErrorsCommand(string[] indexNames, string nodeTag, int? shard)
            {
                _indexNames = indexNames;
                _shard = shard;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                var first = true;
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={Uri.EscapeDataString(indexName)}";
                    first = false;
                }

                if (_shard != null)
                {
                    if (first)
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
                if (response == null ||
                    response.TryGet("Results", out BlittableJsonReaderArray results) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var indexErrors = new IndexErrors[results.Length];
                for (int i = 0; i < results.Length; i++)
                {
                    indexErrors[i] = JsonDeserializationClient.IndexErrors((BlittableJsonReaderObject)results[i]);
                }

                Result = indexErrors;
            }

            public override bool IsReadRequest => true;
        }
    }
}
