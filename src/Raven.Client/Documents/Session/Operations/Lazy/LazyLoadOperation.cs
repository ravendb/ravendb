using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyLoadOperation<T> : ILazyOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly LoadOperation _loadOperation;
        private string[] _ids;
        private string[] _includes;

        public LazyLoadOperation(
            InMemoryDocumentSessionOperations session,
            LoadOperation loadOperation)
        {
            _session = session;
            _loadOperation = loadOperation;
        }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            var idsToCheckOnServer = _ids.Where(id => _session.IsLoadedOrDeleted(id) == false);

            var queryBuilder = new StringBuilder("?");
            _includes.ApplyIfNotNull(include => queryBuilder.AppendFormat("&include={0}", include));
            var hasItems = idsToCheckOnServer.ApplyIfNotNull(id => queryBuilder.AppendFormat("&id={0}", Uri.EscapeDataString(id)));

            if (hasItems == false)
            {
                // no need to hit the server
                Result = _loadOperation.GetDocuments<T>();
                return null;
            }

            return new GetRequest
            {
                Url = "/docs",
                Query = queryBuilder.ToString()
            };
        }

        public LazyLoadOperation<T> ById(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return this;

            if (_ids == null)
                _ids = new[] { id };

            return this;
        }

        public LazyLoadOperation<T> ByIds(IEnumerable<string> ids)
        {
            _ids = ids
                .Where(id => string.IsNullOrWhiteSpace(id) == false)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return this;
        }

        public LazyLoadOperation<T> WithIncludes(string[] includes)
        {
            _includes = includes;
            return this;
        }

        public object Result { get; set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; set; }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            var multiLoadResult = response.Result != null
                ? JsonDeserializationClient.GetDocumentsResult((BlittableJsonReaderObject)response.Result)
                : null;

            HandleResponse(multiLoadResult);
        }

        private void HandleResponse(GetDocumentsResult loadResult)
        {
            _loadOperation.SetResult(loadResult);
            if (RequiresRetry == false)
                Result = _loadOperation.GetDocuments<T>();
        }
    }
}
