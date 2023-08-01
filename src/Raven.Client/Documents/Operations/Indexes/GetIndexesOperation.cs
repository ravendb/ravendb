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
            private readonly string _indexName;

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
            {
                _indexName = indexName;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes";

                if (_indexName != null)
                    url += $"?name={Uri.EscapeDataString(_indexName)}";
                else
                    url += $"?start={_start}&pageSize={_pageSize}";

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
