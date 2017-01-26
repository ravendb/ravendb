using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands.Lazy
{
    public class LazyLoadOperation<T> : ILazyOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly LoadOperation _loadOperation;
        private string[] _ids;
        private string _transformer;
        private string[] _includes;

        public LazyLoadOperation(
            InMemoryDocumentSessionOperations session,
            LoadOperation loadOperation)
        {
            _session = session;
            _loadOperation = loadOperation;
        }

        public GetRequest CreateRequest()
        {
            var idsToCheckOnServer = _ids.Where(id => _session.IsLoadedOrDeleted(id) == false);

            var queryBuilder = new StringBuilder("?");
            _includes.ApplyIfNotNull(include => queryBuilder.AppendFormat("&include={0}", include));
            idsToCheckOnServer.ApplyIfNotNull(id => queryBuilder.AppendFormat("&id={0}", Uri.EscapeDataString(id)));

            if (string.IsNullOrEmpty(_transformer) == false)
                queryBuilder.AppendFormat("&transformer={0}", _transformer);

            return new GetRequest
            {
                Url = "/docs",
                Query = queryBuilder.ToString()
            };
        }

        public LazyLoadOperation<T> ById(string id)
        {
            if (id == null)
                return this;

            if (_ids == null)
                _ids = new[] { id };

            return this;
        }

        public LazyLoadOperation<T> ByIds(IEnumerable<string> ids)
        {
            _ids = ids.ToArray();

            return this;
        }

        public LazyLoadOperation<T> WithIncludes(string[] includes)
        {
            _includes = includes;
            return this;
        }

        public LazyLoadOperation<T> WithTransformer(string transformer)
        {
            _transformer = transformer;
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
                ? JsonDeserializationClient.GetDocumentResult((BlittableJsonReaderObject)response.Result)
                : null;

            HandleResponse(multiLoadResult);
        }

        private void HandleResponse(GetDocumentResult loadResult)
        {
            _loadOperation.SetResult(loadResult);
            if (RequiresRetry == false)
                Result = _loadOperation.GetDocuments<T>();
        }
    }
}
