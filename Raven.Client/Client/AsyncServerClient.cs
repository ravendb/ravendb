using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Client
{
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		long requestCount;
		private readonly string url;
		private readonly DocumentConvention convention;
		private readonly ICredentials credentials;

		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials)
		{
			this.url = url;
			this.convention = convention;
			this.credentials = credentials;
		}

		public void Dispose()
		{
		}

		public IAsyncResult BeginGet(string key, AsyncCallback callback, object state)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return DirectGet(url, key, callback, state);
		}

		public JsonDocument EndGet(IAsyncResult result)
		{
			var asyncData = ((UserAsyncData)result);
			try
			{
				var responseString = asyncData.Request.EndReadResponseString(asyncData.Result);
				return new JsonDocument
				{
					DataAsJson = JObject.Parse(responseString),
					Key = asyncData.Key,
					Etag = new Guid(asyncData.Request.ResponseHeaders["ETag"]),
					Metadata = asyncData.Request.ResponseHeaders.FilterHeaders()
				};
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
				{
					var conflicts = new StreamReader(httpWebResponse.GetResponseStream());
					var conflictsDoc = JObject.Load(new JsonTextReader(conflicts));
					var conflictIds = conflictsDoc.Value<JArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

					throw new ConflictException("Conflict detected on " + asyncData.Key +
												", conflict must be resolved before the document will be accessible")
					{
						ConflictedVersionIds = conflictIds
					};
				}
				throw;
			}
		}

		private UserAsyncData DirectGet(string serverUrl, string key, AsyncCallback callback, object state)
		{
			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, serverUrl + "/docs/" + key, "GET", metadata, credentials);

			return new UserAsyncData(request, request.BeginReadResponseString(callback, state))
			{
				Key =  key
			};
		}

		private class UserAsyncData : IAsyncResult
		{
			public IAsyncResult Result { get; private set; }
			public HttpJsonRequest Request { get; private set; }

			public bool IsCompleted
			{
				get { return Result.IsCompleted; }
			}

			public WaitHandle AsyncWaitHandle
			{
				get { return Result.AsyncWaitHandle; }
			}

			public object AsyncState
			{
				get { return Result.AsyncState; }
			}

			public bool CompletedSynchronously
			{
				get { return Result.CompletedSynchronously; }
			}

			public string Key { get; set; }

			public UserAsyncData(HttpJsonRequest request, IAsyncResult result)
			{
				Request = request;
				Result = result;
			}
		}

		private static void AddTransactionInformation(JObject metadata)
		{
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new JValue(txInfo);
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}
	}
}