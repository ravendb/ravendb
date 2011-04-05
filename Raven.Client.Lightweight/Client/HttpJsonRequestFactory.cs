using System;
using System.Collections.Specialized;
using System.Net;
using System.Runtime.Caching;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;

namespace Raven.Client.Client
{
	///<summary>
	/// Create the HTTP Json Requests to the RavenDB Server
	/// and manages the http cache
	///</summary>
	public class HttpJsonRequestFactory : IDisposable
	{
		/// <summary>
		/// Occurs when a json request is created
		/// </summary>
		public event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate { };

		private MemoryCache cache = new MemoryCache(typeof(HttpJsonRequest).FullName + ".Cache");

		internal int NumOfCachedRequests;

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		public HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, ICredentials credentials,
													 DocumentConvention convention)
		{
			return CreateHttpJsonRequest(self, url, method, new JObject(), credentials, convention);
		}

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		/// <param name="self">The self.</param>
		/// <param name="url">The URL.</param>
		/// <param name="method">The method.</param>
		/// <param name="metadata">The metadata.</param>
		/// <param name="credentials">The credentials.</param>
		/// <param name="convention">The document conventions governing this request</param>
		/// <returns></returns>
		public HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, JObject metadata,
													 ICredentials credentials, DocumentConvention convention)
		{
			var request = new HttpJsonRequest(url, method, metadata, credentials, this);
			ConfigureCaching(url, method, convention, request);
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.WebRequest });
			return request;
		}

		private void ConfigureCaching(string url, string method, DocumentConvention convention, HttpJsonRequest request)
		{
			request.ShouldCacheRequest = convention.ShouldCacheRequest(url);
			if (!request.ShouldCacheRequest || method != "GET")
				return;

			var cachedRequest = (CachedRequest)cache.Get(url);
			if (cachedRequest == null)
				return;
			request.CachedRequestDetails = cachedRequest;
			request.WebRequest.Headers["If-None-Match"] = cachedRequest.Headers["ETag"];
		}


		/// <summary>
		/// Reset the number of cached requests and clear the entire cache
		/// Mostly used for testing
		/// </summary>
		public void ResetCache()
		{
			cache = new MemoryCache(typeof(HttpJsonRequest).FullName + ".Cache");
			NumOfCachedRequests = 0;
		}



		/// <summary>
		/// The number of requests that we got 304 for 
		/// and were able to handle purely from the cache
		/// </summary>
		public int NumberOfCachedRequests
		{
			get { return NumOfCachedRequests; }
		}

		internal string GetCachedResponse(HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot get cached response from a request that has no cached infomration");
			httpJsonRequest.ResponseStatusCode = HttpStatusCode.NotModified;
			httpJsonRequest.ResponseHeaders = new NameValueCollection(httpJsonRequest.CachedRequestDetails.Headers);
			Interlocked.Increment(ref NumOfCachedRequests);
			return httpJsonRequest.CachedRequestDetails.Data;
		}

		internal void CacheResponse(WebResponse response, string text, HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.Method == "GET" && httpJsonRequest.ShouldCacheRequest &&
				string.IsNullOrEmpty(response.Headers["ETag"]) == false)
			{
				cache.Set(httpJsonRequest.Url, new CachedRequest
				{
					Data = text,
					Headers = response.Headers
				}, new CacheItemPolicy()); // cache as much as possible, for as long as possible, using the default cache limits
			}
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			cache.Dispose();
		}
	}
}