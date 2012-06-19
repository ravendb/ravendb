using System;
using System.Collections.Specialized;
using System.Net;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Profiling;
using Raven.Client.Util;
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
		/// Advanced: Don't set this unless you know what you are doing!
		/// 
		/// Enable using basic authentication using http
		/// By default, RavenDB only allows basic authentication over HTTPS, setting this property to true
		/// will instruct RavenDB to make unsecure calls (usually only good for testing / internal networks).
		/// </summary>
		public bool EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers { get; set; }

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
		internal void InvokeLogRequest(IHoldProfilingInformation sender, Func<RequestResultArgs> generateRequentResult)
		{
			var handler = LogRequest;
			if (handler != null) 
				handler(sender, generateRequentResult());
		}

		private readonly int maxNumberOfCachedRequests;
		private SimpleCache<CachedRequest> cache;

		internal int NumOfCachedRequests;

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		public HttpJsonRequest CreateHttpJsonRequest(CreateHttpJsonRequestParams createHttpJsonRequestParams)
		{
			if (disposed)
				throw new ObjectDisposedException(typeof(HttpJsonRequestFactory).FullName);
			var request = new HttpJsonRequest(createHttpJsonRequestParams, this)
			{
				ShouldCacheRequest = createHttpJsonRequestParams.Convention.ShouldCacheRequest(createHttpJsonRequestParams.Url)
			};

			if (request.ShouldCacheRequest && createHttpJsonRequestParams.Method == "GET" && !DisableHttpCaching)
			{
				var cachedRequestDetails = ConfigureCaching(createHttpJsonRequestParams.Url, request.webRequest.Headers.Set);
				request.CachedRequestDetails = cachedRequestDetails.CachedRequest;
				request.SkipServerCheck = cachedRequestDetails.SkipServerCheck;
			}
			ConfigureRequest(createHttpJsonRequestParams.Owner, new WebRequestEventArgs {Request = request.webRequest});
			return request;
		}

		internal CachedRequestOp ConfigureCaching(string url, Action<string, string> setHeader)
		{
			var cachedRequest = cache.Get(url);
			if (cachedRequest == null)
				return new CachedRequestOp { SkipServerCheck = false };
			bool skipServerCheck = false;
			if (AggressiveCacheDuration != null)
			{
				var duration = AggressiveCacheDuration.Value;
				if(duration.TotalSeconds > 0)
					setHeader("Cache-Control", "max-age=" + duration.TotalSeconds);

				if ((DateTimeOffset.Now - cachedRequest.Time) < duration) // can serve directly from local cache
					skipServerCheck = true;
			}

			setHeader("If-None-Match", cachedRequest.Headers["ETag"]);
			return new CachedRequestOp { SkipServerCheck = skipServerCheck, CachedRequest = cachedRequest };
		}


		/// <summary>
		/// Reset the number of cached requests and clear the entire cache
		/// Mostly used for testing
		/// </summary>
		public void ResetCache()
		{
			if (cache != null)
				cache.Dispose();

			cache = new SimpleCache<CachedRequest>(maxNumberOfCachedRequests);
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

		/// <summary>
		/// Determine whether to use compression or not 
		/// </summary>
		public bool DisableRequestCompression { get; set; }

		/// <summary>
		/// default ctor
		/// </summary>
		/// <param name="maxNumberOfCachedRequests"></param>
		public HttpJsonRequestFactory(int maxNumberOfCachedRequests)
		{
			this.maxNumberOfCachedRequests = maxNumberOfCachedRequests;
			ResetCache();
		}

#if !NET35
		///<summary>
		/// The aggressive cache duration
		///</summary>
		public TimeSpan? AggressiveCacheDuration
		{
			get { return aggressiveCacheDuration.Value; }
			set { aggressiveCacheDuration.Value = value; }
		}

		/// <summary>
		/// Disable the HTTP caching
		/// </summary>
		public bool DisableHttpCaching
		{
			get { return disableHttpCaching.Value; }
			set { disableHttpCaching.Value = value; }
		}

		private readonly ThreadLocal<TimeSpan?> aggressiveCacheDuration = new ThreadLocal<TimeSpan?>(() => null);

		private readonly ThreadLocal<bool> disableHttpCaching = new ThreadLocal<bool>(() => false);
#else
		[ThreadStatic] private static TimeSpan? aggressiveCacheDuration;
		[ThreadStatic] private static bool disableHttpCaching;


		
		/// <summary>
		/// Disable the HTTP caching
		/// </summary>
		public bool DisableHttpCaching
		{
			get { return disableHttpCaching; }
			set { disableHttpCaching = value; }
		}

		///<summary>
		/// The aggressive cache duration
		///</summary>
		public TimeSpan? AggressiveCacheDuration
		{
			get { return aggressiveCacheDuration; }
			set { aggressiveCacheDuration = value; }
		}
#endif
		private volatile bool disposed;

		internal RavenJToken GetCachedResponse(HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot get cached response from a request that has no cached information");
			httpJsonRequest.ResponseStatusCode = HttpStatusCode.NotModified;
			httpJsonRequest.ResponseHeaders = new NameValueCollection(httpJsonRequest.CachedRequestDetails.Headers);
			IncrementCachedRequests();
			return httpJsonRequest.CachedRequestDetails.Data.CloneToken();
		}

		internal void IncrementCachedRequests()
		{
			Interlocked.Increment(ref NumOfCachedRequests);
		}

		internal void CacheResponse(string url, RavenJToken data, NameValueCollection headers)
		{
			if (string.IsNullOrEmpty(headers["ETag"])) 
				return;

			var clone = data.CloneToken();
			clone.EnsureSnapshot();
			cache.Set(url, new CachedRequest
			{
				Data = clone,
				Time = DateTimeOffset.Now,
				Headers = new NameValueCollection(headers)
			});
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			if (disposed)
				return;
		    disposed = true;
			cache.Dispose();
#if !NET35
			aggressiveCacheDuration.Dispose();
			disableHttpCaching.Dispose();
#endif
		}

		internal void UpdateCacheTime(HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot update cached response from a request that has no cached information");
			httpJsonRequest.CachedRequestDetails.Time = DateTimeOffset.Now;
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			var oldAgressiveCaching = AggressiveCacheDuration;
			var oldHttpCaching = DisableHttpCaching;

			AggressiveCacheDuration = null;
			DisableHttpCaching = true;

			return new DisposableAction(() =>
			{
				AggressiveCacheDuration = oldAgressiveCaching;
				DisableHttpCaching = oldHttpCaching;
			});
		}
	}
}
