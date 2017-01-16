using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Documents
{
    public class PatchByIndexOperation : IOperation
    {
        private readonly string _indexName;
        private readonly IndexQuery _queryToUpdate;
        private readonly PatchRequest _patch;
        private readonly QueryOperationOptions _options;

        public PatchByIndexOperation(string indexName, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (queryToUpdate == null)
                throw new ArgumentNullException(nameof(queryToUpdate));
            if (patch == null)
                throw new ArgumentNullException(nameof(patch));

            _indexName = indexName;
            _queryToUpdate = queryToUpdate;
            _patch = patch;
            _options = options;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PatchByIndexCommand(conventions, context, _indexName, _queryToUpdate, _patch, _options);
        }

        private class PatchByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly DocumentConvention _conventions;
            private readonly JsonOperationContext _context;
            private readonly string _indexName;
            private readonly IndexQuery _queryToUpdate;
            private readonly BlittableJsonReaderObject _patch;
            private readonly QueryOperationOptions _options;

            public PatchByIndexCommand(DocumentConvention conventions, JsonOperationContext context, string indexName, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));
                if (queryToUpdate == null)
                    throw new ArgumentNullException(nameof(queryToUpdate));
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));

                _conventions = conventions;
                _context = context;
                _indexName = indexName;
                _queryToUpdate = queryToUpdate;
                _patch = new EntityToBlittable(null).ConvertEntityToBlittable(patch, conventions, _context);
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                var u = $"{node.Url}/databases/{node.Database}";
                url = $"{_queryToUpdate.GetIndexQueryUrl(u, _indexName, "queries")}&allowStale=" +
                      $"{_options.AllowStale}&maxOpsPerSec={_options.MaxOpsPerSecond}&details={_options.RetrieveDetails}";

                if (_options.StaleTimeout != null)
                    url += "&staleTimeout=" + _options.StaleTimeout;

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _patch);
                    })
                };

                return request;
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