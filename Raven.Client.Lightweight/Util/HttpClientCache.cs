using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;

using Raven.Abstractions.Connection;

namespace Raven.Client.Util
{
    public class HttpClientCache : IDisposable
    {
        private readonly ConcurrentDictionary<HttpClientCacheKey, ConcurrentStack<Tuple<long, HttpClient>>> cache = new ConcurrentDictionary<HttpClientCacheKey, ConcurrentStack<Tuple<long, HttpClient>>>();

        private readonly Timer cleanupTimer;

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

        public long Count
        {
            get
            {
                return cache.Values.Sum(x => x.Count);
            }
        }

        public HttpClientCache(int maxIdleTimeInMilliseconds)
        {
            MaxIdleTime = maxIdleTimeInMilliseconds;
            cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        internal void Cleanup(object state)
        {
            foreach (var kvp in cache)
            {
                ConcurrentStack<Tuple<long, HttpClient>> stack = kvp.Value;

                if (stack.Count == 0)
                    continue;

                if (stack.Any(item => IsClientTooOld(item.Item1)) == false)
                    continue;

                var newStack = new ConcurrentStack<Tuple<long, HttpClient>>();
                if(cache.TryUpdate(kvp.Key, newStack, stack) == false)
                    continue;

                var tmpStack = new Stack<Tuple<long, HttpClient>>();
                Tuple<long, HttpClient> client;
                while (stack.TryPop(out client))
                {
                    if (IsClientTooOld(client.Item1))
                    {
                        try
                        {
                            client.Item2.Dispose();
                        }
                        catch (Exception)
                        {
                        }
                    }
                    else
                    {
                        tmpStack.Push(client);
                    }
                }
                while (tmpStack.Count > 0)
                {
                    newStack.Push(tmpStack.Pop());
                }
            }
        }

        public HttpClient GetClient(TimeSpan timeout, OperationCredentials credentials, Func<HttpMessageHandler> handlerFactory)
        {
            var key = new HttpClientCacheKey(timeout, credentials);
            var stack = cache.GetOrAdd(key, i => new ConcurrentStack<Tuple<long, HttpClient>>());

            Tuple<long, HttpClient> client;
            while (stack.TryPop(out client))
            {
                if (IsClientTooOld(client.Item1))
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

        private bool IsClientTooOld(long ticks)
        {
            return Stopwatch.GetTimestamp() - ticks >= _maxIdleTimeInStopwatchTicks;
        }

        public void ReleaseClient(HttpClient client, OperationCredentials credentials)
        {
            var key = new HttpClientCacheKey(client.Timeout, credentials);
            var queue = cache.GetOrAdd(key, i => new ConcurrentStack<Tuple<long, HttpClient>>());
            queue.Push(Tuple.Create(Stopwatch.GetTimestamp(), client));
        }

        public void Dispose()
        {
            cleanupTimer.Dispose();

            foreach (var client in cache.Values.SelectMany(queue => queue))
                client.Item2.Dispose();
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
