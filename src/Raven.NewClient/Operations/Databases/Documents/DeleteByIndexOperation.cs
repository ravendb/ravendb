using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Documents
{
    public class DeleteByIndexOperation : IOperation<OperationIdResult>
    {
        private readonly string _indexName;
        private readonly IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        public DeleteByIndexOperation(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (queryToDelete == null)
                throw new ArgumentNullException(nameof(queryToDelete));

            _indexName = indexName;
            _queryToDelete = queryToDelete;
            _options = options;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
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
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));
                if (queryToDelete == null)
                    throw new ArgumentNullException(nameof(queryToDelete));

                _indexName = indexName;
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

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}