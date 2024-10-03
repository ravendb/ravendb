﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Logging;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;

namespace Raven.Client.Http
{
    public sealed class HttpCache : IDisposable, ILowMemoryHandler
    {
        internal const string NotFoundResponse = "404 Response";

        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForClient<HttpCache>();

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
            _unmanagedBuffersPool = new UnmanagedBuffersPool(Logger, nameof(HttpCache), "Client");
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        [Flags]
        public enum ItemFlags
        {
            None = 0,
            NotFound = 1,

            AggressivelyCached = 16
        }

        public sealed unsafe class HttpCacheItem : IDisposable
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
            private int _internallyReleased;

            internal int Usages => _usages;

            public HttpCacheItem()
            {
                _usages = 1;
                LastServerUpdate = SystemTime.UtcNow;
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

                if (Logger.IsDebugEnabled)
                {
                    Logger.Debug($"Released item from cache. Total cache size: {Cache._totalSize}");
                }
            }

            internal void ReleaseRefInternal()
            {
                if (Interlocked.CompareExchange(ref _internallyReleased, 1, 0) != 0)
                {
                    // when we create the HttpCacheItem object, the _usages is set to 1.
                    // when we release it, we need to make sure that it's released by us only once.
                    // we might try to release it twice because of a race condition between releasing an old object and the FreeSpace task.
                    return;
                }

                ReleaseRef();
            }

            public void Dispose()
            {
                ReleaseRefInternal();
            }
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
            if (Interlocked.Add(ref _totalSize, mem.SizeInBytes) > _maxSize)
            {
                if (_isFreeSpaceRunning == false)
                    Task.Run(FreeSpace);
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

            HttpCacheItem old = null;
            _items.AddOrUpdate(url, httpCacheItem, (s, oldItem) =>
            {
                old = oldItem;
                _forTestingPurposes?.OnHttpCacheSetUpdate?.Invoke();
                return httpCacheItem;
            });
            //We need to check if the cache is been disposed after the item was added otherwise we will run into another race condition
            //where it started been disposed right after we checked it and before we managed to insert the new cache item.
            if (_disposing)
            {
                //We might have double release here but we have a protection for that.
                httpCacheItem.ReleaseRefInternal();
            }
            old?.ReleaseRefInternal();
        }

        public void SetNotFound(string url, bool aggressivelyCached)
        {
            var flag = aggressivelyCached ? ItemFlags.AggressivelyCached : ItemFlags.None;
            var httpCacheItem = new HttpCacheItem
            {
                ChangeVector = NotFoundResponse,
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
                _forTestingPurposes?.OnHttpCacheNotFoundUpdate?.Invoke();
                return httpCacheItem;
            });
            //We need to check if the cache is been disposed after the item was added otherwise we will run into another race condition
            //where it started been disposed right after we checked it and before we managed to insert the new cache item.
            if (_disposing)
            {
                //We might have double release here but we have a protection for that.
                httpCacheItem.ReleaseRefInternal();
            }
            old?.ReleaseRefInternal();
        }

        public int Generation;
        private volatile bool _disposing;

        internal void FreeSpace()
        {
            if (_forTestingPurposes?.DisableFreeSpaceCleanup == true)
                return;

            if (_isFreeSpaceRunning.Raise() == false)
                return;

            try
            {
                if (_items.Count == 0)
                    return;

                Debug.Assert(_isFreeSpaceRunning);

                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Started to clear the http cache. Items: {_items.Count:#,#;;0}");

                // Using the current total size will always ensure that under low memory conditions
                // we are making our best effort to actually get some memory back to the system in
                // the worst of conditions.
                var sizeToClear = _totalSize / 2;

                var numberOfClearedItems = 0;
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

                    numberOfClearedItems++;
                    value.ReleaseRefInternal();
                    sizeCleared += value.Size;
                }

                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Cleared {numberOfClearedItems:#,#;;0} items from the http cache, " +
                                 $"size: {new Sparrow.Size(sizeCleared, SizeUnit.Bytes)} " +
                                 $"Total items: {_items.Count:#,#;;0}");
            }
            finally
            {
                _isFreeSpaceRunning.Lower();
            }
        }

        public readonly struct ReleaseCacheItem : IDisposable
        {
            public readonly HttpCacheItem Item;
            private readonly int _cacheGeneration;

            public ReleaseCacheItem(HttpCacheItem item)
            {
                Item = item;
                _cacheGeneration = item.Cache.Generation;
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

            public bool MightHaveBeenModified => Item.Generation != _cacheGeneration;

            public void NotModified()
            {
                if (Item != null)
                {
                    Item.Generation = _cacheGeneration;
                    Item.LastServerUpdate = SystemTime.UtcNow;
                }
            }

            public void Dispose()
            {
                Item?.ReleaseRef();
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
                    if (obj != null)
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

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;
            FreeSpace();
        }

        public void LowMemoryOver()
        {
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            public Action OnHttpCacheSetUpdate;

            public Action OnHttpCacheNotFoundUpdate;

            public bool DisableFreeSpaceCleanup;
        }
    }
}
