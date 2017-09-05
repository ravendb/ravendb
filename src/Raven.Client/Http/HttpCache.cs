using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;

namespace Raven.Client.Http
{
    public class HttpCache : IDisposable, ILowMemoryHandler
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<HttpCache>("Client");

        private readonly long _maxSize;
        private readonly ConcurrentDictionary<string, HttpCacheItem> _items = new ConcurrentDictionary<string, HttpCacheItem>();
        private long _totalSize;
        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        /// <summary>
        /// This value should not be used outside of tests: fetching it locks the cache.
        /// </summary>
        public int NumberOfItems => _items.Count;

        public HttpCache(long maxSize = 1024 * 1024L * 512L)
        {
            _maxSize = maxSize;
            _unmanagedBuffersPool = new UnmanagedBuffersPool(nameof(HttpCache), "Client");
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public unsafe class HttpCacheItem : IDisposable
        {
            public string ChangeVector;
            public byte* Ptr;
            public int Size;
            public DateTime LastServerUpdate;
            public int Usages;
            public int Utilization;
            public int Generation;
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

                if (Allocation != null)
                {
                    Cache._unmanagedBuffersPool.Return(Allocation);
                    Interlocked.Add(ref Cache._totalSize, -Size);
                }
                Allocation = null;
                GC.SuppressFinalize(this);

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Released item from cache. Total cache size: {Cache._totalSize}");
                }
            }

            public void Dispose()
            {
                Release();
            }

            ~HttpCacheItem()
            {
                try
                {
                    Release();
                }
                catch (ObjectDisposedException)
                {
                    // nothing that can be done here
                }
            }

        }

        /// <summary>
        /// Used to check if the FreeSpace routine is running. Avoids creating 
        /// many tasks that shouldn't be run.
        /// </summary>
        private MultipleUseFlag _isFreeSpaceRunning = new MultipleUseFlag();

        public unsafe void Set(string url, string changeVector, BlittableJsonReaderObject result)
        {
            var mem = _unmanagedBuffersPool.Allocate(result.Size);
            result.CopyTo(mem.Address);
            if (Interlocked.Add(ref _totalSize, result.Size) > _maxSize && _isFreeSpaceRunning.Raise())
            {
                Task.Run(() => FreeSpace());
            }
            var httpCacheItem = new HttpCacheItem
            {
                Usages = 1,
                ChangeVector = changeVector,
                Ptr = mem.Address,
                Size = result.Size,
                Allocation = mem,
                LastServerUpdate = SystemTime.UtcNow,
                Cache = this,
                Generation = Generation
            };
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                oldItem.Release();
                return httpCacheItem;
            });
        }

        public void SetNotFound(string url)
        {
            var httpCacheItem = new HttpCacheItem
            {
                Usages = 1,
                ChangeVector = "404 Response",
                Ptr = null,
                Size = 0,
                Allocation = null,
                LastServerUpdate = SystemTime.UtcNow,
                Cache = this,
                Generation = Generation
            };
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                oldItem.Release();
                return httpCacheItem;
            });
        }

        public int Generation;

        private void FreeSpace()
        {
            Debug.Assert(_isFreeSpaceRunning);

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

            _isFreeSpaceRunning.Lower();
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

            public bool MightHaveBeenModified => Item.Generation != Item.Cache.Generation;

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

        public unsafe ReleaseCacheItem Get(JsonOperationContext context, string url, out string changeVector, out BlittableJsonReaderObject obj)
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
                    changeVector = item.ChangeVector;
                    obj = item.Ptr != null ? new BlittableJsonReaderObject(item.Ptr, item.Size, context) : null;
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Url returned from the cache with etag: {changeVector}. {url}.");
                    return releaser;
                }
            }
            obj = null;
            changeVector = null;
            return new ReleaseCacheItem();
        }

        public void Clear()
        {
            // PERF: _items.Values locks the entire dictionary to produce a
            // snapshot. This does not lock.
            foreach (var item in _items)
            {
                HttpCacheItem value;
                if (_items.TryRemove(item.Key, out value) == false)
                    continue;

                value.Dispose();
            }
        }

        public void Dispose()
        {
            foreach (var item in _items)
            {
                item.Value.Dispose();
            }
            _unmanagedBuffersPool.Dispose();
        }

        public void LowMemory()
        {
            if (_isFreeSpaceRunning.Raise())
                FreeSpace();
        }

        public void LowMemoryOver()
        {
        }
    }
}
