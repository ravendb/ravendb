#if !NET_3_5
using System;
using System.Collections.Specialized;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;

namespace Raven.Client.Document.Batches
{
	public class LazyLoadOperation<T> : ILazyOperation
	{
		private readonly string key;
		private readonly LoadOperation loadOperation;

		public LazyLoadOperation(string key, LoadOperation loadOperation)
		{
			this.key = key;
			this.loadOperation = loadOperation;
		}

		public GetRequest CraeteRequest()
		{
			return new GetRequest
			{
				Url = "/docs/"+key
			};
		}

		public object Result { get; set; }

		public bool RequiresRetry { get; set; }

		public void HandleResponse(GetResponse response)
		{
			if(response.Status == 404)
			{
				Result = null;
				RequiresRetry = false;
				return;
			}

			var headers = new NameValueCollection();
			foreach (var header in response.Headers)
			{
				headers[header.Key] = header.Value;
			}
			var jsonDocument = SerializationHelper.DeserializeJsonDocument(key, response.Result, headers, (HttpStatusCode) response.Status);
			RequiresRetry = loadOperation.SetResult(jsonDocument);
			if (RequiresRetry == false)
				Result = loadOperation.Complete<T>();
		}

		public IDisposable EnterContext()
		{
			return loadOperation.EnterLoadContext();
		}
	}
}
#endif