using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Sparrow.Json;
using Raven.NewClient.Client.Json;

namespace Raven.NewClient.Client.Commands.Lazy
{
    public class LazyTransformerLoadOperation<T> : ILazyOperation
    {
        private readonly string[] ids;
        private readonly string transformer;

        private readonly Dictionary<string, object> transformerParameters;

        private readonly LoadTransformerOperation _loadTransformerOperation;
        private readonly bool singleResult;

        public LazyTransformerLoadOperation(string[] ids, string transformer, Dictionary<string, object> transformerParameters, LoadTransformerOperation loadTransformerOperation, bool singleResult)
        {
            this.ids = ids;
            this.transformer = transformer;
            this.transformerParameters = transformerParameters;
            this._loadTransformerOperation = loadTransformerOperation;
            _loadTransformerOperation.ByIds(ids);
            _loadTransformerOperation.WithTransformer(transformer, transformerParameters);
            this.singleResult = singleResult;
        }

        public GetRequest CreateRequest()
        {
            string query = "?" + string.Join("&", ids.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
            if (string.IsNullOrEmpty(transformer) == false)
            {
                query += "&transformer=" + transformer;

                if (transformerParameters != null)
                    query = transformerParameters.Aggregate(query, (current, queryInput) => current + ("&" + string.Format("tp-{0}={1}", queryInput.Key, queryInput.Value)));
            }

            return new GetRequest
            {
                Url = "/docs",
                Query = query
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
            var loadTransformerOperation = JsonDeserializationClient.GetDocumentResult(result);
            HandleResponse(loadTransformerOperation);
        }

        private void HandleResponse(GetDocumentResult loadResult)
        {
            _loadTransformerOperation.SetResult(loadResult);
            if (RequiresRetry == false)
                Result = _loadTransformerOperation.GetTransformedDocuments<T>(loadResult);
        }
    }
}
