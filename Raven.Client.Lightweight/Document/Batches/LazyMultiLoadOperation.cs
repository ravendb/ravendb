#if !NET_3_5
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
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

		public void HandleResponse(GetResponse response)
		{
			var result = RavenJObject.Parse(response.Result);

			var multiLoadResult = new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
			};
			RequiresRetry = loadOperation.SetResult(multiLoadResult);
			if (RequiresRetry == false)
				Result = loadOperation.Complete<T>();
		}

		public IDisposable EnterContext()
		{
			return loadOperation.EnterMultiLoadContext();
		}
	}
}
#endif