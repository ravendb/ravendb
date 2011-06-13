using System;
using System.Collections.Specialized;
using System.Net;
using System.Threading;
using FromMono.System.Runtime.Caching;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
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

		/// <summary>
		/// Occurs when a json request is completed
		/// </summary>
		public event EventHandler<RequestResultArgs> LogRequest = delegate { };

		/// <summary>
		/// Invoke the LogRequest event
		/// </summary>
		internal void InvokeLogRequest(IHoldProfilingInformation sender, RequestResultArgs e)
		{
			var handler = LogRequest;
			if (handler != null) 
				handler(sender, e);
		}

		private MemoryCache cache = new MemoryCache(typeof(HttpJsonRequest).FullName + ".Cache");

		internal int NumOfCachedRequests;

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		public HttpJsonRequest CreateHttpJsonRequest(IHoldProfilingInformation self, string url, string method, ICredentials credentials,
													 DocumentConvention convention)
		{
			return CreateHttpJsonRequest(self, url, method, new RavenJObject(), credentials, convention);
		}

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		public HttpJsonRequest CreateHttpJsonRequest(IHoldProfilingInformation self, string url, string method, RavenJObject metadata,
													 ICredentials credentials, DocumentConvention convention)
		{
			var request = new HttpJsonRequest(url, method, metadata, credentials, this, self);
			ConfigureCaching(url, method, convention, request);
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.webRequest });
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
			if (AggressiveCacheDuration != null)
			{
				var duraion = AggressiveCacheDuration.Value;
				if(duraion.TotalSeconds > 0)
					request.webRequest.Headers["Cache-Control"] = "max-age=" + duraion.TotalSeconds;

				if ((DateTimeOffset.Now - cachedRequest.Time) < duraion) // can serve directly from local cache
					request.SkipServerCheck = true;
			}

			request.CachedRequestDetails = cachedRequest;
			request.webRequest.Headers["If-None-Match"] = cachedRequest.Headers["ETag"];
		}


		/// <summary>
		/// Reset the number of cached requests and clear the entire cache
		/// Mostly used for testing
		/// </summary>
		public void ResetCache()
		{
			if (cache != null)
				cache.Dispose();

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

#if !NET_3_5
		///<summary>
		/// The aggressive cache duration
		///</summary>
		public TimeSpan? AggressiveCacheDuration
		{
			get { return aggressiveCacheDuration.Value; }
			set { aggressiveCacheDuration.Value = value; }
		}

		private readonly ThreadLocal<TimeSpan?> aggressiveCacheDuration = new ThreadLocal<TimeSpan?>(() => null);
#else
		[ThreadStatic] private static TimeSpan? aggressiveCacheDuration;

		///<summary>
		/// The aggressive cache duration
		///</summary>
		public TimeSpan? AggressiveCacheDuration
		{
			get { return aggressiveCacheDuration; }
			set { aggressiveCacheDuration = value; }
		}
#endif

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
					Time = DateTimeOffset.Now,
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

		internal void UpdateCacheTime(HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot update cached response from a request that has no cached infomration");
			httpJsonRequest.CachedRequestDetails.Time = DateTimeOffset.Now;
		}
	}
}