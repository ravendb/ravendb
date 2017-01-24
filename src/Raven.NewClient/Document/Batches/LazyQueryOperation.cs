using System;
using System.Text;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document.Batches
{
    public class LazyQueryOperation<T> : ILazyOperation
    {
        private readonly QueryOperation _queryOperation;
        private readonly Action<QueryResult> _afterQueryExecuted;

        public LazyQueryOperation(QueryOperation queryOperation, Action<QueryResult> afterQueryExecuted)
        {
            _queryOperation = queryOperation;
            _afterQueryExecuted = afterQueryExecuted;
        }

        public GetRequest CreateRequest()
        {
            var stringBuilder = new StringBuilder();
            _queryOperation.IndexQuery.AppendQueryString(stringBuilder);

            var request = new GetRequest
            {
                Url = "/queries/" + _queryOperation.IndexName,
                Query = stringBuilder.ToString()
            };

            return request;
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

    }
}
