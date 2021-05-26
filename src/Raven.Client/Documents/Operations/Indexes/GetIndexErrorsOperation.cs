using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexErrorsOperation : IMaintenanceOperation<IndexErrors[]>
    {
        private readonly string[] _indexNames;
        private readonly string _nodeTag;

        public GetIndexErrorsOperation()
        {
        }

        public GetIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        internal GetIndexErrorsOperation(string[] indexNames, string nodeTag)
        {
            _indexNames = indexNames;
            _nodeTag = nodeTag;
        }

        public RavenCommand<IndexErrors[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexErrorsCommand(_indexNames, _nodeTag);
        }

        private class GetIndexErrorsCommand : RavenCommand<IndexErrors[]>
        {
            private readonly string[] _indexNames;

            public GetIndexErrorsCommand(string[] indexNames)
            {
                _indexNames = indexNames;
            }

            internal GetIndexErrorsCommand(string[] indexNames, string nodeTag)
            {
                _indexNames = indexNames;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={Uri.EscapeDataString(indexName)}";
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
