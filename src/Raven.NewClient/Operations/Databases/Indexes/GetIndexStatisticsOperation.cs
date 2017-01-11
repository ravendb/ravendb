using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Data.Indexes;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
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

        public RavenCommand<IndexStats> GetCommand()
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

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var results = JsonDeserializationClient.GetIndexStatisticsResponse(response).Results;
                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}