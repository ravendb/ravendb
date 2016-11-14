using System.Net.Http;
using System;
using System.Collections.Specialized;
using System.Net;
using System.Net.Http.Headers;
using System.Threading;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Connection
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

        private int maxNumberOfCachedRequests;

        internal readonly HttpClientCache httpClientCache;
        public HttpClientCache HttpClientCache
        {
            get { return httpClientCache; }
        }

        internal readonly Func<HttpMessageHandler> httpMessageHandler;

        internal readonly bool acceptGzipContent;

        internal readonly string authenticationScheme;

        private SimpleCache cache;

        internal int NumOfCachedRequests;

        /// <summary>
        /// Creates the HTTP json request.
        /// </summary>
        public HttpJsonRequest CreateHttpJsonRequest(CreateHttpJsonRequestParams createHttpJsonRequestParams)
        {
            if (disposed)
                throw new ObjectDisposedException(typeof(HttpJsonRequestFactory).FullName);

            if (RequestTimeout != null)
                createHttpJsonRequestParams.Timeout = RequestTimeout.Value;

            var request = new HttpJsonRequest(createHttpJsonRequestParams, this)
            {
                ShouldCacheRequest =
                    createHttpJsonRequestParams.AvoidCachingRequest == false &&
                    createHttpJsonRequestParams.ShouldCacheRequest(createHttpJsonRequestParams.Url)
            };

            if (request.ShouldCacheRequest && !DisableHttpCaching)
            {
                var cachedRequestDetails = ConfigureCaching(createHttpJsonRequestParams.Url, request.AddHeader);
                request.CachedRequestDetails = cachedRequestDetails.CachedRequest;
                request.SkipServerCheck = cachedRequestDetails.SkipServerCheck;
            }

            ConfigureRequest(createHttpJsonRequestParams.Owner, new WebRequestEventArgs { Client = request.httpClient, Credentials = createHttpJsonRequestParams.Credentials });
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
                if (duration.TotalSeconds > 0)
                    setHeader("Cache-Control", "max-age=" + duration.TotalSeconds);

                if (cachedRequest.ForceServerCheck == false && (SystemTime.UtcNow - cachedRequest.Time) < duration) // can serve directly from local cache
                    skipServerCheck = true;

                cachedRequest.ForceServerCheck = false;
            }

            setHeader("If-None-Match", cachedRequest.Headers[Constants.MetadataEtagField]);
            return new CachedRequestOp { SkipServerCheck = skipServerCheck, CachedRequest = cachedRequest };
        }


        /// <summary>
        /// Reset the number of cached requests and clear the entire cache
        /// Mostly used for testing
        /// </summary>
        public void ResetCache(int? newMaxNumberOfCachedRequests = null)
        {

            if (newMaxNumberOfCachedRequests != null && newMaxNumberOfCachedRequests.Value == maxNumberOfCachedRequests)
                return;

            if (cache != null)
                cache.Dispose();

            if (newMaxNumberOfCachedRequests != null)
            {
                maxNumberOfCachedRequests = newMaxNumberOfCachedRequests.Value;
            }
            cache = new SimpleCache(maxNumberOfCachedRequests);
            NumOfCachedRequests = 0;
        }

        public void ExpireItemsFromCache(string db)
        {
            cache.ForceServerCheckOfCachedItemsForDatabase(db);
            Interlocked.Increment(ref numberOfCacheResets);
        }

        /// <summary>
        /// The number of cache evictions forced by
        /// tracking changes if aggressive cache was enabled
        /// </summary>
        public int NumberOfCacheResets
        {
            get
            {
                return Volatile.Read(ref numberOfCacheResets);
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
        /// <param name="httpMessageHandler"></param>
        /// <param name="acceptGzipContent"></param>
        /// <param name="authenticationScheme"></param>
        public HttpJsonRequestFactory(int maxNumberOfCachedRequests, Func<HttpMessageHandler> httpMessageHandler = null, bool acceptGzipContent = true, string authenticationScheme = null)
        {
            this.maxNumberOfCachedRequests = maxNumberOfCachedRequests;
            this.httpMessageHandler = httpMessageHandler;
            this.acceptGzipContent = acceptGzipContent;
            this.authenticationScheme = authenticationScheme;

            var maxIdleTime = 100 * 1000;//this matches ServicePointManager.MaxServicePointIdleTime

            httpClientCache = new HttpClientCache(maxIdleTime);

            ResetCache();
        }

        private readonly AsyncLocal<TimeSpan?> aggressiveCacheDuration = new AsyncLocal<TimeSpan?>();

        ///<summary>
        /// The aggressive cache duration
        ///</summary>
        public TimeSpan? AggressiveCacheDuration
        {
            get
            {
                return aggressiveCacheDuration.Value;
            }
            set
            {
                aggressiveCacheDuration.Value = value;
            }
        }

        private readonly AsyncLocal<TimeSpan?> requestTimeout = new AsyncLocal<TimeSpan?>();

        ///<summary>
        /// Session timeout - Thread Local
        ///</summary>
        public TimeSpan? RequestTimeout
        {
            get
            {
                return requestTimeout.Value;
            }
            set
            {
                requestTimeout.Value = value;
            }
        }

        private readonly AsyncLocal<bool> disableHttpCaching = new AsyncLocal<bool>();

        /// <summary>
        /// Disable the HTTP caching
        /// </summary>
        public bool DisableHttpCaching
        {
            get
            {
                return disableHttpCaching.Value;
            }
            set
            {
                disableHttpCaching.Value = value;
            }
        }

        private volatile bool disposed;
        private int numberOfCacheResets;

        internal RavenJToken GetCachedResponse(HttpJsonRequest httpJsonRequest, NameValueCollection additionalHeaders)
        {
            if (httpJsonRequest.CachedRequestDetails == null)
                throw new InvalidOperationException("Cannot get cached response from a request that has no cached information");
            httpJsonRequest.ResponseStatusCode = HttpStatusCode.NotModified;
            httpJsonRequest.ResponseHeaders = new NameValueCollection(httpJsonRequest.CachedRequestDetails.Headers);

            httpJsonRequest.ResponseHeaders.Remove(Constants.Headers.RavenForcePrimaryServerCheck);
            if (additionalHeaders != null && additionalHeaders[Constants.Headers.RavenForcePrimaryServerCheck] != null)
            {
                httpJsonRequest.ResponseHeaders.Set(Constants.Headers.RavenForcePrimaryServerCheck, additionalHeaders[Constants.Headers.RavenForcePrimaryServerCheck]);
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
                string forcePrimaryServerCHeck = additionalHeaders.GetFirstValue(Constants.Headers.RavenForcePrimaryServerCheck);
                if (forcePrimaryServerCHeck != null)
                    httpJsonRequest.ResponseHeaders.Add(Constants.Headers.RavenForcePrimaryServerCheck, forcePrimaryServerCHeck);
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
            if (string.IsNullOrEmpty(headers[Constants.MetadataEtagField]))
                return;

            RavenJToken clone;
            if (data == null)
            {
                clone = null;
            }
            else
            {
                clone = data.CloneToken();
                clone.EnsureCannotBeChangeAndEnableSnapshotting();
            }

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
            httpClientCache.Dispose();
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
