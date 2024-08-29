using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public sealed class GetIndexesOperation : IMaintenanceOperation<IndexDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetIndexesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<IndexDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesCommand(_start, _pageSize);
        }

        internal sealed class GetIndexesCommand : RavenCommand<IndexDefinition[]>
        {
            private readonly int _start;
            private readonly int _pageSize;
            private readonly string[] _indexNames;

            public GetIndexesCommand(int start, int pageSize)
                : this(start, pageSize, nodeTag: null)
            {
            }

            internal GetIndexesCommand(int start, int pageSize, string nodeTag)
            {
                _start = start;
                _pageSize = pageSize;
                SelectedNodeTag = nodeTag;
            }
            
            internal GetIndexesCommand(string indexName, string nodeTag) 
                : this([indexName], nodeTag)
            {
            }

            internal GetIndexesCommand(string[] indexNames, string nodeTag)
            {
                _indexNames = indexNames;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?";

                if (_indexNames is { Length: > 0 })
                {
                    for (int i = 0; i < _indexNames.Length; i++)
                    {
                        string indexName = _indexNames[i];
                        if (i > 0)
                            url += "&";

                        url += $"name={Uri.EscapeDataString(indexName)}";
                    }
                }
                else
                    url += $"start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}
