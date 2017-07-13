using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyQueryOperation<T> : ILazyOperation
    {
        private readonly DocumentConventions _conventions;
        private readonly QueryOperation _queryOperation;
        private readonly Action<QueryResult> _afterQueryExecuted;

        public LazyQueryOperation(DocumentConventions conventions, QueryOperation queryOperation, Action<QueryResult> afterQueryExecuted)
        {
            _conventions = conventions;
            _queryOperation = queryOperation;
            _afterQueryExecuted = afterQueryExecuted;
        }

        public GetRequest CreateRequest()
        {
            return new GetRequest
            {
                Url = "/queries",
                Method = HttpMethod.Post,
                Content = new IndexQueryContent(_conventions, _queryOperation.IndexQuery)
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

            var queryResult = JsonDeserializationClient.QueryResult((BlittableJsonReaderObject)response.Result);

            HandleResponse(queryResult);
        }

        private void HandleResponse(QueryResult queryResult)
        {
            _queryOperation.EnsureIsAcceptableAndSaveResult(queryResult);

            _afterQueryExecuted?.Invoke(queryResult);
            Result = _queryOperation.Complete<T>();
            QueryResult = queryResult;
        }

        private class IndexQueryContent : GetRequest.IContent
        {
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery _query;

            public IndexQueryContent(DocumentConventions conventions, IndexQuery query)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _query = query ?? throw new ArgumentNullException(nameof(query));
            }

            public void WriteContent(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                writer.WriteIndexQuery(_conventions, context, _query);
            }
        }
    }
}
