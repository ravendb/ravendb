using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyTransformerLoadOperation<T> : ILazyOperation
    {
        private readonly string[] _ids;
        private readonly string _transformer;

        private readonly Dictionary<string, object> _transformerParameters;

        private readonly LoadTransformerOperation _loadTransformerOperation;

        public LazyTransformerLoadOperation(string[] ids, string transformer, Dictionary<string, object> transformerParameters, LoadTransformerOperation loadTransformerOperation)
        {
            _ids = ids;
            _transformer = transformer;
            _transformerParameters = transformerParameters;
            _loadTransformerOperation = loadTransformerOperation;
            _loadTransformerOperation.ByIds(ids);
            _loadTransformerOperation.WithTransformer(transformer, transformerParameters);
        }

        public GetRequest CreateRequest(JsonOperationContext ctx)
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
