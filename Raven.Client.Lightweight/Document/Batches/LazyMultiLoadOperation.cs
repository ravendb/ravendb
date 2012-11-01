using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
#if !SILVERLIGHT
using Raven.Client.Shard;
#endif
using Raven.Json.Linq;

namespace Raven.Client.Document.Batches
{
	public class LazyMultiLoadOperation<T> : ILazyOperation
	{
		private readonly MultiLoadOperation loadOperation;
		private readonly string[] ids;
		private readonly string[] includes;

		public LazyMultiLoadOperation(
			MultiLoadOperation loadOperation,
			string[] ids, 
			string[] includes)
		{
			this.loadOperation = loadOperation;
			this.ids = ids;
			this.includes = includes;
		}

		public GetRequest CraeteRequest()
		{
			string query = "?";
			if (includes != null && includes.Length > 0)
			{
				query += string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			query += "&" + string.Join("&", ids.Select(x => "id=" + x).ToArray());
			return new GetRequest
			{
				Url = "/queries/",
				Query = query 
			};
		}

		public object Result { get; set; }
		public bool RequiresRetry { get; set; }

#if !SILVERLIGHT
		public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
		{
			var list = new List<MultiLoadResult>(
				from response in responses
				let result = response.Result
				select new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				});

			var capacity = list.Max(x => x.Results.Count);

			var finalResult = new MultiLoadResult
			                  	{
									Includes = new List<RavenJObject>(),
			                  		Results = new List<RavenJObject>(Enumerable.Range(0,capacity).Select(x=> (RavenJObject)null))
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
#endif

		public void HandleResponse(GetResponse response)
		{
			var result = response.Result;

			var multiLoadResult = new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
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

#if !SILVERLIGHT
		public object ExecuteEmbedded(IDatabaseCommands commands)
		{
			return commands.Get(ids, includes);
		}

		public void HandleEmbeddedResponse(object result)
		{
			HandleResponse((MultiLoadResult) result);
		}
#endif
	}
}