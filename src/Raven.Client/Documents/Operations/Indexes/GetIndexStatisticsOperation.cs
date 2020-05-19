using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexStatisticsOperation : IMaintenanceOperation<IndexStats>
    {
        private readonly string _indexName;

        public GetIndexStatisticsOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<IndexStats> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexStatisticsCommand(_indexName);
        }

        private class GetIndexStatisticsCommand : RavenCommand<IndexStats>
        {
            private readonly string _indexName;

            public GetIndexStatisticsCommand(string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats?name={Uri.EscapeDataString(_indexName)}";

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
                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}