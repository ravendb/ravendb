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

			return new GetRequest
			{
				Url = "/queries/",
				Query = string.Join("&", ids.Select(x => "id=" + x).ToArray()) + "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray())
			};
		}

		public object Result { get; set; }
		public bool RequiresRetry { get; set; }

		public void HandleResponse(GetResponse response)
		{
			if (response.Status != 200)
				throw new InvalidOperationException("Unknown status code from server: " + response.Status);
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