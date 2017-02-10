using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Data.Indexes;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class GetIndexStatisticsOperation : IAdminOperation<IndexStats>
    {
        private readonly string _indexName;

        public GetIndexStatisticsOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<IndexStats> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetIndexStatisticsCommand(_indexName);
        }

        private class GetIndexStatisticsCommand : RavenCommand<IndexStats>
        {
            private readonly string _indexName;

            public GetIndexStatisticsCommand(string indexName)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));
                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats?name={Uri.EscapeUriString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
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