using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Shard;
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
	public class LazyMultiLoadOperation<T> : ILazyOperation
	{
		private readonly MultiLoadOperation loadOperation;
		private readonly string[] ids;
		private readonly string transformer;
		private readonly KeyValuePair<string, Type>[] includes;

		public LazyMultiLoadOperation(
			MultiLoadOperation loadOperation,
			string[] ids,
			KeyValuePair<string, Type>[] includes,
			string transformer = null)
		{
			this.loadOperation = loadOperation;
			this.ids = ids;
			this.includes = includes;
			this.transformer = transformer;
		}

		public GetRequest CreateRequest()
		{
			string query = "?";
			if (includes != null && includes.Length > 0)
			{
				query += string.Join("&", includes.Select(x => "include=" + x.Key).ToArray());
			}
			query += "&" + string.Join("&", ids.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
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

		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var list = new List<MultiLoadResult>(
				from response in responses
				let result = response.Result
				select new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                    Results = result.Value<RavenJArray>("Results").Select(x => x as RavenJObject).ToList()
				});

			var capacity = list.Max(x => x.Results.Count);

			var finalResult = new MultiLoadResult
			{
				Includes = new List<RavenJObject>(),
				Results = new List<RavenJObject>(Enumerable.Range(0, capacity).Select(x => (RavenJObject) null))
			};


			foreach (var multiLoadResult in list)
			{
				finalResult.Includes.AddRange(multiLoadResult.Includes);

				for (int i = 0; i < multiLoadResult.Results.Count; i++)
				{
					if (finalResult.Results[i] == null)
						finalResult.Results[i] = multiLoadResult.Results[i];
				}
			}
			RequiresRetry = loadOperation.SetResult(finalResult);
			if (RequiresRetry == false)
				Result = loadOperation.Complete<T>();

		}

		public void HandleResponse(GetResponse response)
		{
			var result = response.Result;

			var multiLoadResult = new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = result.Value<RavenJArray>("Results").Select(x=>x as RavenJObject).ToList()
			};
			HandleResponse(multiLoadResult);
		}

		private void HandleResponse(MultiLoadResult multiLoadResult)
		{
			RequiresRetry = loadOperation.SetResult(multiLoadResult);
			if (RequiresRetry == false)
				Result = loadOperation.Complete<T>();
		}

		public IDisposable EnterContext()
		{
			return loadOperation.EnterMultiLoadContext();
		}

		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
			return commands.Get(ids, includePaths, transformer);
		}

		public void HandleEmbeddedResponse(object result)
		{
			HandleResponse((MultiLoadResult) result);
		}
	}
}