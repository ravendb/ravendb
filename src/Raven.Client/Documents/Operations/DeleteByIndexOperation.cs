using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DeleteByIndexOperation : IOperation<OperationIdResult>
    {
        private readonly string _indexName;
        private readonly IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        public DeleteByIndexOperation(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            if (queryToDelete == null)
                throw new ArgumentNullException(nameof(queryToDelete));

            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _queryToDelete = queryToDelete;
            _options = options;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteByIndexCommand(_indexName, _queryToDelete, _options);
        }

        private class DeleteByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly string _indexName;
            private readonly IndexQuery _queryToDelete;
            private readonly QueryOperationOptions _options;

            public DeleteByIndexCommand(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
            {
                if (queryToDelete == null)
                    throw new ArgumentNullException(nameof(queryToDelete));

                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _queryToDelete = queryToDelete;
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                var u = $"{node.Url}/databases/{node.Database}";
                url = $"{_queryToDelete.GetIndexQueryUrl(u, _indexName, "queries")}&allowStale=" +
                      $"{_options.AllowStale}&maxOpsPerSec={_options.MaxOpsPerSecond}&details={_options.RetrieveDetails}";

                if (_options.StaleTimeout != null)
                    url += "&staleTimeout=" + _options.StaleTimeout;

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}