using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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

        public HttpCache(long maxSize)
        {
            _maxSize = maxSize;
            _unmanagedBuffersPool = new UnmanagedBuffersPool(nameof(HttpCache), "Client");
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        [Flags]
        public enum ItemFlags
        {
            None = 0,
            NotFound = 1,

            AggressivelyCached = 16
        }

        public unsafe class HttpCacheItem : IDisposable
        {
            public string ChangeVector;
            public byte* Ptr;
            public int Size;
            public DateTime LastServerUpdate;
            public ItemFlags Flags;
            public int Generation;
            public AllocatedMemoryData Allocation;
            public HttpCache Cache;

            private int _usages;

            public HttpCacheItem()
            {
                this._usages = 1;
                this.LastServerUpdate = SystemTime.UtcNow;
            }

            public bool AddRef()
            {
                // May return false if the memory has been already released.
                var result = Interlocked.Increment(ref _usages) > 1;
                if (result == false)
                    ReleaseRef();
                return result;
            }

            public void ReleaseRef()
            {
                // Check if we are ready to return the memory
                if (Interlocked.Decrement(ref _usages) > 0)
                    return;

                // Check if someone haven't entered here yet. 
                if (Interlocked.CompareExchange(ref _usages, -(1000 * 1000), 0) != 0)
                    return;

                // Do the actual deallocation. 
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
                ReleaseRef();
            }

#if !RELEASE
            ~HttpCacheItem()
            {

                // Hitting this on DEBUG and/or VALIDATE and getting a higher number than 0 means we have a leak.
                // On release we will leak, but wont crash. 
                if (_usages > 0)
                    throw new LowMemoryException("Detected a leak on HttpCache when running the finalizer. See: http://issues.hibernatingrhinos.com/issue/RavenDB-9737");
        }
#endif
        }

        /// <summary>
        /// Used to check if the FreeSpace routine is running. Avoids creating 
        /// many tasks that shouldn't be run.
        /// </summary>
        private readonly MultipleUseFlag _isFreeSpaceRunning = new MultipleUseFlag();

        public unsafe void Set(string url, string changeVector, BlittableJsonReaderObject result)
        {
#if DEBUG
            result.BlittableValidation();
#endif 
            var mem = _unmanagedBuffersPool.Allocate(result.Size);
            result.CopyTo(mem.Address);
            if (Interlocked.Add(ref _totalSize, result.Size) > _maxSize)
            {
                Task.Run(() => FreeSpace());
            }

            var httpCacheItem = new HttpCacheItem
            {
                ChangeVector = changeVector,
                Ptr = mem.Address,
                Size = result.Size,
                Allocation = mem,
                Cache = this,
                Generation = Generation
            };

            HttpCacheItem old=null;
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                old = oldItem;
                return httpCacheItem;
            });
            //We need to check if the cache is been disposed after the item was added otherwise we will run into another race condition
            //where it started been disposed right after we checked it and before we managed to insert the new cache item.
            if (_disposing)
            {
                //We might have double release here but we have a protection for that.
                httpCacheItem.ReleaseRef();
            }
            old?.ReleaseRef();
        }

        public void SetNotFound(string url, bool aggressivelyCached)
        {
            var flag = aggressivelyCached ? ItemFlags.AggressivelyCached : ItemFlags.None;
            var httpCacheItem = new HttpCacheItem
            {
                ChangeVector = "404 Response",
                Ptr = null,
                Size = 0,
                Allocation = null,
                Cache = this,
                Generation = Generation,
                Flags = ItemFlags.NotFound | flag
            };
            HttpCacheItem old = null;
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                old = oldItem;
                return httpCacheItem;
            });
            //We need to check if the cache is been disposed after the item was added otherwise we will run into another race condition
            //where it started been disposed right after we checked it and before we managed to insert the new cache item.
            if (_disposing)
            {
                //We might have double release here but we have a protection for that.
                httpCacheItem.ReleaseRef();
            }
            old?.ReleaseRef();
        }

        public int Generation;
        private volatile bool _disposing;

        private void FreeSpace()
        {
            if (!_isFreeSpaceRunning.Raise())
                return;

            try
            {
                Debug.Assert(_isFreeSpaceRunning);                
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Started to clear the http cache. Items: {_items.Count}");

                // Using the current total size will always ensure that under low memory conditions
                // we are making our best effort to actually get some memory back to the system in
                // the worst of conditions.
                var sizeToClear = _totalSize / 2;

                var sizeCleared = 0L;
                var start = SystemTime.UtcNow;
                foreach (var item in _items)
                {
                    // We are aggressively targeting whatever it is in our hands as 
                    // long as it haven't been touched since we started to free space.
                    var lastServerUpdate = item.Value.LastServerUpdate;
                    if (lastServerUpdate > start)
                        continue;

                    // In case that we have already achieved out target, only free those 
                    // items not having been accessed in the last minute.
                    if (sizeCleared > sizeToClear)
                    {
                        var itemAge = start - lastServerUpdate;
                        if (itemAge.TotalMinutes <= 1)
                            continue;
                    }

                    // We remove the item because there is no grounds to reject it.
                    if (_items.TryRemove(item.Key, out var value) == false)
                        continue;

                    // We explicitly ignore the case of a cached value
                    // that was changed while we are clearing free space
                    // the result of such a scenario is early eviction of
                    // a value from the cache. Not enough for us to worry
                    // about.

                    value.ReleaseRef();
                    sizeCleared += value.Size;
                }
            }
            finally
            {
                _isFreeSpaceRunning.Lower();
            }            
        }

        public struct ReleaseCacheItem : IDisposable
        {
            public readonly HttpCacheItem Item;

            public ReleaseCacheItem(HttpCacheItem item)
            {
                Item = item;
            }

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
                this.Item?.ReleaseRef();
            }
        }

        public unsafe ReleaseCacheItem Get(JsonOperationContext context, string url, out string changeVector, out BlittableJsonReaderObject obj)
        {
            if (_items.TryGetValue(url, out var item))
            {
                if (item.AddRef())
                {
                    var releaser = new ReleaseCacheItem(item);

                    changeVector = item.ChangeVector;

                    obj = item.Ptr != null ? new BlittableJsonReaderObject(item.Ptr, item.Size, context) : null;
#if DEBUG
                    if(obj != null)
                    {
                        obj.BlittableValidation();
                    }
#endif
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
                if (_items.TryRemove(item.Key, out var value) == false)
                    continue;

                value.Dispose();
            }
        }

        public void Dispose()
        {
            _disposing = true;
            foreach (var item in _items)
            {
                item.Value.Dispose();
            }
            _unmanagedBuffersPool.Dispose();
        }

        public void LowMemory()
        {
            FreeSpace();
        }

        public void LowMemoryOver()
        {
        }
    }
}
