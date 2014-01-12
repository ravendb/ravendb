using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Shard;
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
#if !SILVERLIGHT
	public class LazyTransformerLoadOperation<T> : ILazyOperation
	{
		private readonly string[] ids;
		private readonly string transformer;
		private readonly LoadTransformerOperation loadTransformerOperation;
		private readonly bool singleResult;

		public LazyTransformerLoadOperation(string[] ids, string transformer, LoadTransformerOperation loadTransformerOperation, bool singleResult)
		{
			this.ids = ids;
			this.transformer = transformer;
			this.loadTransformerOperation = loadTransformerOperation;
			this.singleResult = singleResult;
		}

		public GetRequest CreateRequest()
		{
			string query = "?" + string.Join("&", ids.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
			if (!string.IsNullOrEmpty(transformer))
				query += "&transformer=" + transformer;
			return new GetRequest
			{
				Url = "/queries/",
				Query = query
			};
		}

		public object Result { get; set; }

		public QueryResult QueryResult { get; set; }

		public bool RequiresRetry { get; set; }

#if !SILVERLIGHT
		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var response = responses.OrderBy(x => x.Status).First(); // this way, 200 response is higher than 404
			HandleResponse(response);
		}
#endif

		public void HandleResponse(GetResponse response)
		{
			if (response.RequestHasErrors())
			{
				throw new InvalidOperationException("Got bad status code: " + response.Status);
			}

			HandleRespose(new MultiLoadResult
			{
				Includes = response.Result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = response.Result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
			});
		}

		public IDisposable EnterContext()
		{
			return null;
		}

		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.Get(ids, null, transformer);
		}

		public void HandleEmbeddedResponse(object result)
		{
			var multiLoadResult = (MultiLoadResult) result;
			HandleRespose(multiLoadResult);
		}

		private void HandleRespose(MultiLoadResult multiLoadResult)
		{
			T[] complete = loadTransformerOperation.Complete<T>(multiLoadResult);
			Result = singleResult ? (object) complete[0] : complete;
		}
	}
#endif
}