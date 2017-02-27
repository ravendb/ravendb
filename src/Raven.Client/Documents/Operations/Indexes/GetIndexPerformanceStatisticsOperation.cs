using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
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

        public RavenCommand<IndexPerformanceStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexPerformanceStatisticsCommand(conventions, _indexNames);
        }

        private class GetIndexPerformanceStatisticsCommand : RavenCommand<IndexPerformanceStats[]>
        {
            private readonly DocumentConventions _conventions;
            private readonly string[] _indexNames;

            public GetIndexPerformanceStatisticsCommand(DocumentConventions conventions, string[] indexNames)
            {
                _conventions = conventions;
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

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                ThrowInvalidResponse();
            }

            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var stats = new IndexPerformanceStats[response.Length];
                for (var i = 0; i < response.Length; i++)
                {
                    stats[i] = (IndexPerformanceStats)_conventions.DeserializeEntityFromBlittable(typeof(IndexPerformanceStats), (BlittableJsonReaderObject)response[i]);
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
                        url += $"?name={Uri.EscapeDataString(indexName)}";
                    else
                        url += $"&name={Uri.EscapeDataString(indexName)}";

                    first = false;
                }

                return url;
            }

            public override bool IsReadRequest => true;
        }
    }
}