using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;


namespace Raven.NewClient.Client.Http
{
    public class HttpCache : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<HttpCache>("Client");

        private readonly long _maxSize;
        private readonly ConcurrentDictionary<string, HttpCacheItem> _items = new ConcurrentDictionary<string, HttpCacheItem>();
        private long _totalSize;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        public int NumberOfItems => _items.Count;

        public HttpCache(long maxSize = 1024 * 1024L * 512L)
        {
            _maxSize = maxSize;
            _unmanagedBuffersPool = new UnmanagedBuffersPool(nameof(HttpCache), "Client");
        }

        public unsafe class HttpCacheItem : IDisposable
        {
            public long Etag;
            public byte* Ptr;
            public int Size;
            public DateTime LastServerUpdate;
            public int Usages;
            public int Utilization;
            public AllocatedMemoryData Allocation;
            public HttpCache Cache;

            public bool AddRef()
            {
                Interlocked.Increment(ref Utilization);
                return Interlocked.Increment(ref Usages) > 1;
            }

            public void Release()
            {
                if (Interlocked.Decrement(ref Usages) > 0)
                    return;

                if (Interlocked.CompareExchange(ref Usages, -(1000 * 1000), 0) != 0)
                    return;

                Cache._unmanagedBuffersPool.Return(Allocation);
                Interlocked.Add(ref Cache._totalSize, -Size);
                Allocation = null;
#if DEBUG
                GC.SuppressFinalize(this);
#endif

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Released item from cache. Total cache size: {Cache._totalSize}");
                }
            }

            public void Dispose()
            {
                Release();
            }

#if DEBUG
            ~HttpCacheItem()
            {
                throw new InvalidOperationException("Did not release memory for cache item");
            }
#endif
        }

        private Task _cleanupTask;

        public unsafe void Set(string url, long etag, BlittableJsonReaderObject result)
        {
            var mem = _unmanagedBuffersPool.Allocate(result.Size);
            result.CopyTo((byte*)mem.Address);
            if (Interlocked.Add(ref _totalSize, result.Size) > _maxSize)
            {
                if (_cleanupTask == null)
                {
                    var cleanup = new Task(FreeSpace);
                    if (Interlocked.CompareExchange(ref _cleanupTask, cleanup, null) == null)
                    {
                        cleanup.ContinueWith(_ => Interlocked.Exchange(ref _cleanupTask, null));
                        cleanup.Start();
                    }
                }
            }
            var httpCacheItem = new HttpCacheItem
            {
                Usages = 1,
                Etag = etag,
                Ptr = (byte*)mem.Address,
                Size = result.Size,
                Allocation = mem,
                LastServerUpdate = SystemTime.UtcNow,
                Cache = this,
            };
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                oldItem.Release();
                return httpCacheItem;
            });
        }

        private void FreeSpace()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Started to clear the http cache. Items: {_items.Count}");

            var sizeCleared = 0L;
            var sizeToClear = _maxSize / 4;
            var start = SystemTime.UtcNow;

            var items = _items.OrderBy(x => x.Value.LastServerUpdate)
                .ToList();

            foreach (var item in items)
            {
                var lastServerUpdate = item.Value.LastServerUpdate;
                if (lastServerUpdate > start)
                    continue;

                var itemAge = SystemTime.UtcNow - lastServerUpdate;
                if (sizeCleared > sizeToClear)
                {
                    if (itemAge.TotalMinutes <= 1)
                        break; // we already met the quota, and we are in new items, just drop it

                    if (item.Value.Utilization == 0)
                    {
                        sizeCleared += FreeItem(item);
                    }
                }
                else
                {
                    sizeCleared += FreeItem(item);
                }
            }
        }

        private long FreeItem(KeyValuePair<string, HttpCacheItem> item)
        {
            HttpCacheItem value;
            if (_items.TryRemove(item.Key, out value) == false)
            {
                return 0;
            }
            value.Release();
            if (item.Value != value)
            {
                item.Value.Release();
                return item.Value.Size + value.Size;
            }
            return value.Size;
        }

        public struct ReleaseCacheItem : IDisposable
        {
            public HttpCacheItem Item;

            public TimeSpan Age
            {
                get
                {
                    if (Item == null)
                        return TimeSpan.MaxValue;
                    return SystemTime.UtcNow - Item.LastServerUpdate;
                }
            }

            public void NotModified()
            {
                if (Item != null)
                {
                    Item.LastServerUpdate = SystemTime.UtcNow;
                }
            }

            public void Dispose()
            {
                if (Item != null)
                {
                    Item.Release();
                    Item = null;
                }
            }
        }

        public unsafe ReleaseCacheItem Get(JsonOperationContext context, string url, out long etag, out BlittableJsonReaderObject obj)
        {
            HttpCacheItem item;
            if (_items.TryGetValue(url, out item))
            {
                if (item.AddRef())
                {
                    var releaser = new ReleaseCacheItem
                    {
                        Item = item
                    };
                    etag = item.Etag;
                    obj = new BlittableJsonReaderObject(item.Ptr, item.Size, context);
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Url returned from the cache with etag: {etag}. {url}.");
                    return releaser;
                }
            }
            obj = null;
            etag = 0;
            return new ReleaseCacheItem();
        }

        public void Clear()
        {
            foreach (var key in _items.Keys)
            {
                HttpCacheItem value;
                if (_items.TryRemove(key, out value) == false)
                    continue;

                value.Dispose();
            }
        }

        public void Dispose()
        {
            foreach (var item in _items.Values)
            {
                item.Dispose();
            }
            _unmanagedBuffersPool.Dispose();
        }
    }
}