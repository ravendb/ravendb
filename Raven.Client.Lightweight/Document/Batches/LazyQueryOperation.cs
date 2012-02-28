using System;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Json.Linq;

#if !NET_3_5

namespace Raven.Client.Document.Batches
{
	public class LazyQueryOperation<T> : ILazyOperation
	{
		private readonly QueryOperation queryOperation;
		private readonly Action<QueryResult> afterQueryExecuted;

		public LazyQueryOperation(QueryOperation queryOperation, Action<QueryResult> afterQueryExecuted)
		{
			this.queryOperation = queryOperation;
			this.afterQueryExecuted = afterQueryExecuted;
		}

		public GetRequest CraeteRequest()
		{
			var stringBuilder = new StringBuilder();
			queryOperation.IndexQuery.AppendQueryString(stringBuilder);

			return new GetRequest
			{
				Url = "/indexes/" + queryOperation.IndexName,
				Query = stringBuilder.ToString()
			};
		}

		public object Result { get; set; }

		public bool RequiresRetry { get; set; }

		public void HandleResponse(GetResponse response)
		{
			if (response.Status == 404)
				throw new InvalidOperationException("There is no index named: " + queryOperation.IndexName);
			var json = RavenJObject.Parse(response.Result);
			var queryResult = SerializationHelper.ToQueryResult(json, response.Headers["ETag"]);
			RequiresRetry = queryOperation.IsAcceptable(queryResult) == false;
			if (RequiresRetry) 
				return;

			if (afterQueryExecuted != null)
				afterQueryExecuted(queryResult);
			Result = queryOperation.Complete<T>();
		}

		public IDisposable EnterContext()
		{
			return queryOperation.EnterQueryContext();
		}
	}
}

#endif