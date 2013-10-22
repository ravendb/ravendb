#if !NETFX_CORE && !SILVERLIGHT
using System;
#if SILVERLIGHT || NETFX_CORE
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using System.Collections.Specialized;
#endif
using System.Net;
using System.Net.Http.Headers;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Json.Linq;
using System.Linq;

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
		internal void InvokeLogRequest(IHoldProfilingInformation sender, Func<RequestResultArgs> generateRequestResult)
		{
			var handler = LogRequest;
			if (handler != null) 
				handler(sender, generateRequestResult());
		}

		private readonly int maxNumberOfCachedRequests;
		private SimpleCache cache;

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
				ShouldCacheRequest =
					createHttpJsonRequestParams.AvoidCachingRequest == false && 
					createHttpJsonRequestParams.Convention.ShouldCacheRequest(createHttpJsonRequestParams.Url)
			};

			if (request.ShouldCacheRequest && createHttpJsonRequestParams.Method == "GET" && !DisableHttpCaching)
			{
				var cachedRequestDetails = ConfigureCaching(createHttpJsonRequestParams.Url, request.AddHeader);
				request.CachedRequestDetails = cachedRequestDetails.CachedRequest;
				request.SkipServerCheck = cachedRequestDetails.SkipServerCheck;
			}

			if (RequestTimeout != null)
				request.Timeout = RequestTimeout.Value;

			ConfigureRequest(createHttpJsonRequestParams.Owner, new WebRequestEventArgs { Request = request.webRequest });
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

				if (cachedRequest.ForceServerCheck == false && (SystemTime.UtcNow- cachedRequest.Time) < duration) // can serve directly from local cache
					skipServerCheck = true;

				cachedRequest.ForceServerCheck = false;
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

			cache = new SimpleCache(maxNumberOfCachedRequests);
			NumOfCachedRequests = 0;
		}

		public void ExpireItemsFromCache(string db)
		{
			cache.ForceServerCheckOfCachedItemsForDatabase(db);
			NumberOfCacheResets++;
		}

		/// <summary>
		/// The number of cache evictions forced by
		/// tracking changes if aggressive cache was enabled
		/// </summary>
		public int NumberOfCacheResets
		{
			get { return numberOfCacheResets; }
			private set
			{
				numberOfCacheResets = value;
			}
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
		/// The number of currently held requests in the cache
		/// </summary>
		public int CurrentCacheSize
		{
			get { return cache.CurrentSize; }
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

		///<summary>
		/// The aggressive cache duration
		///</summary>
		public TimeSpan? AggressiveCacheDuration
		{
			get { return aggressiveCacheDuration.Value; }
			set { aggressiveCacheDuration.Value = value; }
		}

		///<summary>
		/// Session timeout - Thread Local
		///</summary>
		public TimeSpan? RequestTimeout {
			get { return requestTimeout.Value; }
			set { requestTimeout.Value = value; }
		}

		/// <summary>
		/// Disable the HTTP caching
		/// </summary>
		public bool DisableHttpCaching
		{
			get { return disableHttpCaching.Value; }
			set { disableHttpCaching.Value = value; }
		}

		/// <summary>
		/// Advanced: Don't set this unless you know what you are doing!
		/// 
		/// Enable using basic authentication using http
		/// By default, RavenDB only allows basic authentication over HTTPS, setting this property to true
		/// will instruct RavenDB to make unsecured calls (usually only good for testing / internal networks).
		/// </summary>
		public bool EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers { get; set; }

		private readonly ThreadLocal<TimeSpan?> aggressiveCacheDuration = new ThreadLocal<TimeSpan?>(() => null);
		private readonly ThreadLocal<TimeSpan?> requestTimeout = new ThreadLocal<TimeSpan?>(() => null);
		private readonly ThreadLocal<bool> disableHttpCaching = new ThreadLocal<bool>(() => false);

		private volatile bool disposed;
		private volatile int numberOfCacheResets;

		internal RavenJToken GetCachedResponse(HttpJsonRequest httpJsonRequest, NameValueCollection additionalHeaders)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot get cached response from a request that has no cached information");
			httpJsonRequest.ResponseStatusCode = HttpStatusCode.NotModified;
			httpJsonRequest.ResponseHeaders = new NameValueCollection(httpJsonRequest.CachedRequestDetails.Headers);

			if (additionalHeaders != null && additionalHeaders[Constants.RavenForcePrimaryServerCheck] != null)
			{
				httpJsonRequest.ResponseHeaders.Add(Constants.RavenForcePrimaryServerCheck, additionalHeaders[Constants.RavenForcePrimaryServerCheck]);
			}

			IncrementCachedRequests();
			return httpJsonRequest.CachedRequestDetails.Data.CloneToken();
		}

		internal RavenJToken GetCachedResponse(HttpJsonRequest httpJsonRequest, HttpResponseHeaders additionalHeaders)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot get cached response from a request that has no cached information");
			httpJsonRequest.ResponseStatusCode = HttpStatusCode.NotModified;
			httpJsonRequest.ResponseHeaders = new NameValueCollection(httpJsonRequest.CachedRequestDetails.Headers);

			if (additionalHeaders != null)
			{
				string forcePrimaryServerCHeck = additionalHeaders.GetFirstValue(Constants.RavenForcePrimaryServerCheck);
				if (forcePrimaryServerCHeck != null)
					httpJsonRequest.ResponseHeaders.Add(Constants.RavenForcePrimaryServerCheck, forcePrimaryServerCHeck);
			}

			IncrementCachedRequests();
			return httpJsonRequest.CachedRequestDetails.Data.CloneToken();
		}

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
			clone.EnsureCannotBeChangeAndEnableSnapshotting();

			cache.Set(url, new CachedRequest
			{
				Data = clone,
				Time = SystemTime.UtcNow,
				Headers = new NameValueCollection(headers),
				Database = MultiDatabase.GetDatabaseName(url)
			});
		}

		internal void CacheResponse(string url, RavenJToken data, HttpResponseHeaders headers)
		{
			if (headers.ETag == null || string.IsNullOrEmpty(headers.ETag.Tag))
				return;

			var clone = data.CloneToken();
			clone.EnsureCannotBeChangeAndEnableSnapshotting();

			cache.Set(url, new CachedRequest
			{
				Data = clone,
				Time = SystemTime.UtcNow,
				Headers = new NameValueCollection(),// TODO: Use headers
				Database = MultiDatabase.GetDatabaseName(url)
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
			aggressiveCacheDuration.Dispose();
			disableHttpCaching.Dispose();
			requestTimeout.Dispose();
		}

		internal void UpdateCacheTime(HttpJsonRequest httpJsonRequest)
		{
			if (httpJsonRequest.CachedRequestDetails == null)
				throw new InvalidOperationException("Cannot update cached response from a request that has no cached information");
			httpJsonRequest.CachedRequestDetails.Time = SystemTime.UtcNow;
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			var oldAggressiveCaching = AggressiveCacheDuration;
			var oldHttpCaching = DisableHttpCaching;

			AggressiveCacheDuration = null;
			DisableHttpCaching = true;

			return new DisposableAction(() =>
			{
				AggressiveCacheDuration = oldAggressiveCaching;
				DisableHttpCaching = oldHttpCaching;
			});
		}
	}
}
#endif