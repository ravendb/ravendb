using System;
using System.Text;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;


namespace Raven.NewClient.Client.Commands.Lazy
{
    public class LazyLoadOperation<T> : ILazyOperation
    {
        private readonly LoadOperation _loadOperation;
        private readonly string[] _ids;
        private readonly string _transformer;
        private readonly string[] _includes;

        public LazyLoadOperation(
            LoadOperation loadOperation,
            string[] ids,
            string[] includes,
            string transformer = null)
        {
            this._loadOperation = loadOperation;
            this._ids = ids;
            this._includes = includes;
            this._transformer = transformer;
        }

        public LazyLoadOperation(
            LoadOperation loadOperation,
            string id)
        {
            this._loadOperation = loadOperation;
            _ids = new[] { id };
        }

        public GetRequest CreateRequest()
        {
            var queryBuilder = new StringBuilder("?");
            _includes.ApplyIfNotNull(include => queryBuilder.AppendFormat("&include={0}", include));
            _ids.ApplyIfNotNull(id => queryBuilder.AppendFormat("&id={0}", Uri.EscapeDataString(id)));

            if (string.IsNullOrEmpty(_transformer) == false)
                queryBuilder.AppendFormat("&transformer={0}", _transformer);

            return new GetRequest
            {
                Url = "/docs",
                Query = queryBuilder.ToString()
            };
        }

        public object Result { get; set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; set; }

        public void HandleResponse(BlittableJsonReaderObject response)
        {
            bool forceRetry;
            response.TryGet("ForceRetry", out forceRetry);

            if (forceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            BlittableJsonReaderObject result;
            response.TryGet("Result", out result);
            var multiLoadResult = JsonDeserializationClient.GetDocumentResult(result);
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
