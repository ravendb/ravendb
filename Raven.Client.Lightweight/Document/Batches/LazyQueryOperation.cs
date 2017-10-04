using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Shard;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Client.Document.Batches
{
    public class LazyQueryOperation<T> : ILazyOperation
    {
        private readonly QueryOperation queryOperation;
        private readonly Action<QueryResult> afterQueryExecuted;
        private readonly HashSet<string> includes;

        private NameValueCollection headers;

        public LazyQueryOperation(QueryOperation queryOperation, Action<QueryResult> afterQueryExecuted, HashSet<string> includes, NameValueCollection headers)
        {
            this.queryOperation = queryOperation;
            this.afterQueryExecuted = afterQueryExecuted;
            this.includes = includes;
            this.headers = headers;
        }

        public GetRequest CreateRequest()
        {
            var stringBuilder = new StringBuilder();
            queryOperation.IndexQuery.AppendQueryString(stringBuilder);

            foreach (var include in includes)
            {
                stringBuilder.Append("&include=").Append(include);
            }
            var request = new GetRequest
            {
                Url = "/indexes/" + queryOperation.IndexName, 
                Query = stringBuilder.ToString()
            };
            if (headers != null)
            {
                foreach (var headerKey in headers.Keys)
                {
                    request.Headers[headerKey.ToString()] = headers[headerKey.ToString()];
                }
            }
            return request;
        }

        public object Result { get; set; }

        public QueryResult QueryResult { get; set; }

        public bool RequiresRetry { get; set; }
        public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
        {
            var count = responses.Count(x => x.Status == 404);
            if (count != 0)
            {
                throw new InvalidOperationException("There is no index named: " + queryOperation.IndexName + " in " + count + " shards");
            }

            var list = responses
                .Select(response => SerializationHelper.ToQueryResult((RavenJObject)response.Result, response.Headers["Temp-Request-Time"], -1))
                .ToList();

            var queryResult = shardStrategy.MergeQueryResults(queryOperation.IndexQuery, list);

            RequiresRetry = queryOperation.IsAcceptable(queryResult) == false;
            if (RequiresRetry)
                return;

            if (afterQueryExecuted != null)
                afterQueryExecuted(queryResult);
            Result = queryOperation.Complete<T>();
            QueryResult = queryResult;
        }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            if (response.Status == 404)
                throw new InvalidOperationException("There is no index named: " + queryOperation.IndexName + Environment.NewLine + response.Result);
            var json = (RavenJObject)response.Result;
            var queryResult = SerializationHelper.ToQueryResult(json, response.Headers["Temp-Request-Time"], -1);
            HandleResponse(queryResult);
        }

        private void HandleResponse(QueryResult queryResult)
        {
            RequiresRetry = queryOperation.IsAcceptable(queryResult) == false;
            if (RequiresRetry)
                return;

            if (afterQueryExecuted != null)
                afterQueryExecuted(queryResult);
            Result = queryOperation.Complete<T>();
            QueryResult = queryResult;
        }

        public IDisposable EnterContext()
        {
            return queryOperation.EnterQueryContext();
        }

        public void SetHeaders(NameValueCollection theHeaders)
        {
            headers = theHeaders;
        }
    }
}
