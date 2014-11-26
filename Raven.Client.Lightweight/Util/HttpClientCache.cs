using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;

using Raven.Abstractions.Connection;

namespace Raven.Client.Util
{
	public class HttpClientCache : IDisposable
	{
		private readonly ConcurrentDictionary<HttpClientCacheKey, ConcurrentQueue<HttpClient>> cache = new ConcurrentDictionary<HttpClientCacheKey, ConcurrentQueue<HttpClient>>();

		public HttpClient GetClient(TimeSpan timeout, OperationCredentials credentials, Func<HttpMessageHandler> handlerFactory)
		{
			var key = new HttpClientCacheKey(timeout, credentials);
			var queue = cache.GetOrAdd(key, i => new ConcurrentQueue<HttpClient>());

			HttpClient client;
			if (queue.TryDequeue(out client))
			{
				client.CancelPendingRequests();
				client.DefaultRequestHeaders.Clear();
				return client;
			}

			return new HttpClient(handlerFactory())
				   {
					   Timeout = timeout
				   };
		}

		public void ReleaseClient(HttpClient client, OperationCredentials credentials)
		{
			var key = new HttpClientCacheKey(client.Timeout, credentials);
			var queue = cache.GetOrAdd(key, i => new ConcurrentQueue<HttpClient>());
			queue.Enqueue(client);
		}

		public void Dispose()
		{
			foreach (var client in cache.Values.SelectMany(queue => queue))
				client.Dispose();
		}

		private class HttpClientCacheKey
		{
			public HttpClientCacheKey(TimeSpan timeout, OperationCredentials credentials)
			{
				Timeout = timeout;
				Credentials = credentials != null ? credentials.Credentials : null;
				ApiKey = credentials != null ? credentials.ApiKey : null;
				AuthenticationDisabled = credentials == null;
			}

			private bool Equals(HttpClientCacheKey other)
			{
				return string.Equals(ApiKey, other.ApiKey) && Equals(Credentials, other.Credentials) && Timeout.Equals(other.Timeout) && AuthenticationDisabled.Equals(other.AuthenticationDisabled);
			}

			public override bool Equals(object obj)
			{
				if (ReferenceEquals(null, obj))
				{
					return false;
				}
				if (ReferenceEquals(this, obj))
				{
					return true;
				}
				if (obj.GetType() != GetType())
				{
					return false;
				}
				return Equals((HttpClientCacheKey)obj);
			}

			public override int GetHashCode()
			{
				unchecked
				{
					int hashCode = (ApiKey != null ? ApiKey.GetHashCode() : 0);
					hashCode = (hashCode * 397) ^ (Credentials != null ? Credentials.GetHashCode() : 0);
					hashCode = (hashCode * 397) ^ Timeout.GetHashCode();
					hashCode = (hashCode * 397) ^ AuthenticationDisabled.GetHashCode();
					return hashCode;
				}
			}

			private bool AuthenticationDisabled { get; set; }

			private TimeSpan Timeout { get; set; }

			private ICredentials Credentials { get; set; }

			private string ApiKey { get; set; }
		}
	}
}