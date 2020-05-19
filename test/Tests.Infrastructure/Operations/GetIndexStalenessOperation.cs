using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations
{
    public class GetIndexStalenessOperation : IMaintenanceOperation<GetIndexStalenessOperation.IndexStaleness>
    {
        private readonly string _indexName;

        public GetIndexStalenessOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<IndexStaleness> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexStalenessCommand(conventions, _indexName);
        }

        private class GetIndexStalenessCommand : RavenCommand<IndexStaleness>
        {
            private readonly DocumentConventions _conventions;
            private readonly string _indexName;

            public GetIndexStalenessCommand(DocumentConventions conventions, string indexName)
            {
                _conventions = conventions;
                _indexName = indexName;
            }

            public override bool IsReadRequest => true;


            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/staleness?name={_indexName}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = (IndexStaleness)_conventions.Serialization.DeserializeEntityFromBlittable(typeof(IndexStaleness), response);
            }
        }

        public class IndexStaleness
        {
            public bool IsStale { get; set; }

            public List<string> StalenessReasons { get; set; }
        }
    }
}
