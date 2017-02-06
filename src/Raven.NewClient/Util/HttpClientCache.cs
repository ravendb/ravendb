using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;

using Raven.NewClient.Abstractions.Connection;

namespace Raven.NewClient.Client.Util
{
    public class HttpClientCache : IDisposable
    {
        private readonly ConcurrentDictionary<HttpClientCacheKey, ConcurrentQueue<Tuple<long, HttpClient>>> cache = new ConcurrentDictionary<HttpClientCacheKey, ConcurrentQueue<Tuple<long, HttpClient>>>();

        private long _maxIdleTime;
        private long _maxIdleTimeInStopwatchTicks;

        /// <summary>
        /// The maximum idle time to keep a connection in the cache (in milliseconds)
        /// </summary>
        public long MaxIdleTime
        {
            get { return _maxIdleTime; }
            set
            {
                _maxIdleTime = value;
                _maxIdleTimeInStopwatchTicks = (long)((value / 1000.0) * Stopwatch.Frequency);
            }
        }

        public HttpClientCache(int maxIdleTimeInMilliseconds)
        {
            MaxIdleTime = maxIdleTimeInMilliseconds;
        }

        public HttpClient GetClient(TimeSpan timeout, OperationCredentials credentials, Func<HttpMessageHandler> handlerFactory)
        {
            var key = new HttpClientCacheKey(timeout, credentials);
            var queue = cache.GetOrAdd(key, i => new ConcurrentQueue<Tuple<long, HttpClient>>());

            Tuple<long, HttpClient> client;
            while (queue.TryDequeue(out client))
            {
                if (Stopwatch.GetTimestamp() - client.Item1 >= _maxIdleTimeInStopwatchTicks)
                {
                    client.Item2.Dispose();
                    continue;
                }
                client.Item2.CancelPendingRequests();
                client.Item2.DefaultRequestHeaders.Clear();
                return client.Item2;
            }

            return new HttpClient(handlerFactory())
            {
                Timeout = timeout
            };
        }

        public void ReleaseClient(HttpClient client, OperationCredentials credentials)
        {
            var key = new HttpClientCacheKey(client.Timeout, credentials);
            var queue = cache.GetOrAdd(key, i => new ConcurrentQueue<Tuple<long, HttpClient>>());
            queue.Enqueue(Tuple.Create(Stopwatch.GetTimestamp(), client));
        }

        public void Dispose()
        {
            foreach (var client in cache.Values.SelectMany(queue => queue))
                client.Item2.Dispose();
        }

        private class HttpClientCacheKey
        {
            public HttpClientCacheKey(TimeSpan timeout, OperationCredentials credentials)
            {
                Timeout = timeout;
                ApiKey = credentials != null ? credentials.ApiKey : null;
                AuthenticationDisabled = credentials == null;
            }

            private bool Equals(HttpClientCacheKey other)
            {
                return string.Equals(ApiKey, other.ApiKey) && Timeout.Equals(other.Timeout) && AuthenticationDisabled.Equals(other.AuthenticationDisabled);
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
                    hashCode = (hashCode * 397) ^ Timeout.GetHashCode();
                    hashCode = (hashCode * 397) ^ AuthenticationDisabled.GetHashCode();
                    return hashCode;
                }
            }

            private bool AuthenticationDisabled { get; set; }

            private TimeSpan Timeout { get; set; }

            private string ApiKey { get; set; }
        }
    }
}
