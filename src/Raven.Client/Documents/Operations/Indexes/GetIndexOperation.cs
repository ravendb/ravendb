using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexOperation : IMaintenanceOperation<IndexDefinition>
    {
        private readonly string _indexName;

        public GetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<IndexDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexCommand(_indexName);
        }

        private class GetIndexCommand : RavenCommand<IndexDefinition>
        {
            private readonly string _indexName;

            public GetIndexCommand(string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}