using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Data.Indexes;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class GetIndexPerformanceStatisticsOperation : IAdminOperation<IndexPerformanceStats[]>
    {
        private readonly string[] _indexNames;

        public GetIndexPerformanceStatisticsOperation()
        {
        }

        public GetIndexPerformanceStatisticsOperation(string[] indexNames)
        {
            if (indexNames == null)
                throw new ArgumentNullException(nameof(indexNames));

            _indexNames = indexNames;
        }

        public RavenCommand<IndexPerformanceStats[]> GetCommand()
        {
            return new GetIndexPerformanceStatisticsCommand(_indexNames);
        }

        private class GetIndexPerformanceStatisticsCommand : RavenCommand<IndexPerformanceStats[]>
        {
            private readonly string[] _indexNames;

            public GetIndexPerformanceStatisticsCommand(string[] indexNames)
            {
                _indexNames = indexNames;
                ResponseType = RavenCommandResponseType.Array;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = GetUrl(node);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                ThrowInvalidResponse();
            }

            public override void SetResponse(BlittableJsonReaderArray response)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var stats = new IndexPerformanceStats[response.Length];
                for (int i = 0; i < response.Length; i++)
                {
                    throw new NotImplementedException("StackOverflow");
                    //stats[i] = JsonDeserializationClient.IndexPerformanceStats((BlittableJsonReaderObject)response[i]);
                }

                Result = stats;
            }

            private string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/indexes/performance";

                if (_indexNames == null)
                    return url;

                var first = true;
                foreach (var indexName in _indexNames)
                {
                    if (first)
                        url += $"?name={Uri.EscapeUriString(indexName)}";
                    else
                        url += $"&name={Uri.EscapeUriString(indexName)}";

                    first = false;
                }

                return url;
            }
        }
    }
}