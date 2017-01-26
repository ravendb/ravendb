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
        private readonly string[] _ids;
        private readonly string _transformer;

        private readonly Dictionary<string, object> _transformerParameters;

        private readonly LoadTransformerOperation _loadTransformerOperation;
        private readonly bool _singleResult;

        public LazyTransformerLoadOperation(string[] ids, string transformer, Dictionary<string, object> transformerParameters, LoadTransformerOperation loadTransformerOperation, bool singleResult)
        {
            _ids = ids;
            _transformer = transformer;
            _transformerParameters = transformerParameters;
            _loadTransformerOperation = loadTransformerOperation;
            _loadTransformerOperation.ByIds(ids);
            _loadTransformerOperation.WithTransformer(transformer, transformerParameters);
            _singleResult = singleResult;
        }

        public GetRequest CreateRequest()
        {
            string query = "?" + string.Join("&", _ids.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
            if (string.IsNullOrEmpty(_transformer) == false)
            {
                query += "&transformer=" + _transformer;

                if (_transformerParameters != null)
                    query = _transformerParameters.Aggregate(query, (current, queryInput) => current + ("&" + string.Format("tp-{0}={1}", queryInput.Key, queryInput.Value)));
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
            _loadTransformerOperation.SetResult(loadResult);
            if (RequiresRetry == false)
                Result = _loadTransformerOperation.GetTransformedDocuments<T>(loadResult);
        }
    }
}
