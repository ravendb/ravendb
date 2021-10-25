// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Taken from: https://raw.githubusercontent.com/dotnet/runtime/main/src/libraries/Microsoft.Extensions.Caching.Memory/src/MemoryCache.cs
// However, we need to add additional features (iteration, explicitly clearing it, etc)

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Utils
{
    public class MemoryCache : IMemoryCache
    {
        internal readonly ILogger _logger;

        private readonly MemoryCacheOptions _options;
        private readonly ConcurrentDictionary<object, CacheEntry> _entries;

        private long _cacheSize;
        private bool _disposed;
        private DateTimeOffset _lastExpirationScan;

        /// <summary>
        /// Creates a new <see cref="MemoryCache"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor)
            : this(optionsAccessor, NullLoggerFactory.Instance)
        {
        }

        /// <summary>
        /// Creates a new <see cref="MemoryCache"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        /// <param name="loggerFactory">The factory used to create loggers.</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor, ILoggerFactory loggerFactory)
        {
            if (optionsAccessor == null)
            {
                throw new ArgumentNullException(nameof(optionsAccessor));
            }

            if (loggerFactory == null)
            {
                throw new ArgumentNullException(nameof(loggerFactory));
            }

            _options = optionsAccessor.Value;
            _logger = loggerFactory.CreateLogger<MemoryCache>();

            _entries = new ConcurrentDictionary<object, CacheEntry>();

            if (_options.Clock == null)
            {
                _options.Clock = new SystemClock();
            }

            _lastExpirationScan = _options.Clock.UtcNow;
        }

        /// <summary>
        /// Cleans up the background collection events.
        /// </summary>
        ~MemoryCache() => Dispose(false);

        /// <summary>
        /// Gets the count of the current entries for diagnostic purposes.
        /// </summary>
        public int Count => _entries.Count;

        // internal for testing
        internal long Size { get => Interlocked.Read(ref _cacheSize); }


        private ICollection<KeyValuePair<object, CacheEntry>> EntriesCollection => _entries;

        public void Clear() => _entries.Clear();

        public IEnumerable<KeyValuePair<object, object>> EntriesForDebug => _entries.Select(kvp => new KeyValuePair<object, object>(kvp.Key, kvp.Value.Value));

        /// <inheritdoc />
        public ICacheEntry CreateEntry(object key)
        {
            CheckDisposed();
            ValidateCacheKey(key);

            return new CacheEntry(key, this);
        }

        internal void SetEntry(CacheEntry entry)
        {
            if (_disposed)
            {
                // No-op instead of throwing since this is called during CacheEntry.Dispose
                return;
            }

            if (_options.SizeLimit.HasValue && !entry.Size.HasValue)
            {
                throw new InvalidOperationException("The cache must have a size value");
            }

            DateTimeOffset utcNow = _options.Clock.UtcNow;

            // Applying the option's absolute expiration only if it's not already smaller.
            // This can be the case if a dependent cache entry has a smaller value, and
            // it was set by cascading it to its parent.
            if (entry.AbsoluteExpirationRelativeToNow.HasValue)
            {
                var absoluteExpiration = utcNow + entry.AbsoluteExpirationRelativeToNow.Value;
                if (!entry.AbsoluteExpiration.HasValue || absoluteExpiration < entry.AbsoluteExpiration.Value)
                {
                    entry.AbsoluteExpiration = absoluteExpiration;
                }
            }

            // Initialize the last access timestamp at the time the entry is added
            entry.LastAccessed = utcNow;

            if (_entries.TryGetValue(entry.Key, out CacheEntry priorEntry))
            {
                priorEntry.SetExpired(EvictionReason.Replaced);
            }

            if (entry.CheckExpired(utcNow))
            {
                entry.InvokeEvictionCallbacks();
                if (priorEntry != null)
                {
                    RemoveEntry(priorEntry);
                }

                StartScanForExpiredItemsIfNeeded(utcNow);
                return;
            }

            bool exceedsCapacity = UpdateCacheSizeExceedsCapacity(entry);
            if (!exceedsCapacity)
            {
                bool entryAdded = false;

                if (priorEntry == null)
                {
                    // Try to add the new entry if no previous entries exist.
                    entryAdded = _entries.TryAdd(entry.Key, entry);
                }
                else
                {
                    // Try to update with the new entry if a previous entries exist.
                    entryAdded = _entries.TryUpdate(entry.Key, entry, priorEntry);

                    if (entryAdded)
                    {
                        if (_options.SizeLimit.HasValue)
                        {
                            // The prior entry was removed, decrease the by the prior entry's size
                            Interlocked.Add(ref _cacheSize, -priorEntry.Size.Value);
                        }
                    }
                    else
                    {
                        // The update will fail if the previous entry was removed after retrival.
                        // Adding the new entry will succeed only if no entry has been added since.
                        // This guarantees removing an old entry does not prevent adding a new entry.
                        entryAdded = _entries.TryAdd(entry.Key, entry);
                    }
                }

                if (entryAdded)
                {
                    entry.AttachTokens();
                }
                else
                {
                    if (_options.SizeLimit.HasValue)
                    {
                        // Entry could not be added, reset cache size
                        Interlocked.Add(ref _cacheSize, -entry.Size.Value);
                    }

                    entry.SetExpired(EvictionReason.Replaced);
                    entry.InvokeEvictionCallbacks();
                }

                if (priorEntry != null)
                {
                    priorEntry.InvokeEvictionCallbacks();
                }
            }
            else
            {
                entry.SetExpired(EvictionReason.Capacity);
                TriggerOvercapacityCompaction();
                entry.InvokeEvictionCallbacks();
                if (priorEntry != null)
                {
                    RemoveEntry(priorEntry);
                }
            }

            StartScanForExpiredItemsIfNeeded(utcNow);
        }

        /// <inheritdoc />
        public bool TryGetValue(object key, out object result)
        {
            ValidateCacheKey(key);
            CheckDisposed();

            DateTimeOffset utcNow = _options.Clock.UtcNow;

            if (_entries.TryGetValue(key, out CacheEntry entry))
            {
                // Check if expired due to expiration tokens, timers, etc. and if so, remove it.
                // Allow a stale Replaced value to be returned due to concurrent calls to SetExpired during SetEntry.
                if (!entry.CheckExpired(utcNow) || entry.EvictionReason == EvictionReason.Replaced)
                {
                    entry.LastAccessed = utcNow;
                    result = entry.Value;

                    StartScanForExpiredItemsIfNeeded(utcNow);

                    return true;
                }
                else
                {
                    // TODO: For efficiency queue this up for batch removal
                    RemoveEntry(entry);
                }
            }

            StartScanForExpiredItemsIfNeeded(utcNow);

            result = null;
            return false;
        }

        /// <inheritdoc />
        public void Remove(object key)
        {
            ValidateCacheKey(key);

            CheckDisposed();
            if (_entries.TryRemove(key, out CacheEntry entry))
            {
                if (_options.SizeLimit.HasValue)
                {
                    Interlocked.Add(ref _cacheSize, -entry.Size.Value);
                }

                entry.SetExpired(EvictionReason.Removed);
                entry.InvokeEvictionCallbacks();
            }

            StartScanForExpiredItemsIfNeeded(_options.Clock.UtcNow);
        }

        private void RemoveEntry(CacheEntry entry)
        {
            if (EntriesCollection.Remove(new KeyValuePair<object, CacheEntry>(entry.Key, entry)))
            {
                if (_options.SizeLimit.HasValue)
                {
                    Interlocked.Add(ref _cacheSize, -entry.Size.Value);
                }

                entry.InvokeEvictionCallbacks();
            }
        }

        internal void EntryExpired(CacheEntry entry)
        {
            // TODO: For efficiency consider processing these expirations in batches.
            RemoveEntry(entry);
            StartScanForExpiredItemsIfNeeded(_options.Clock.UtcNow);
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StartScanForExpiredItemsIfNeeded(DateTimeOffset utcNow)
        {
            if (_options.ExpirationScanFrequency < utcNow - _lastExpirationScan)
            {
                ScheduleTask(utcNow);
            }

            void ScheduleTask(DateTimeOffset utcNow)
            {
                _lastExpirationScan = utcNow;
                Task.Factory.StartNew(state => ScanForExpiredItems((MemoryCache)state), this,
                    CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
            }
        }

        private static void ScanForExpiredItems(MemoryCache cache)
        {
            DateTimeOffset now = cache._lastExpirationScan = cache._options.Clock.UtcNow;

            foreach (KeyValuePair<object, CacheEntry> item in cache._entries)
            {
                CacheEntry entry = item.Value;

                if (entry.CheckExpired(now))
                {
                    cache.RemoveEntry(entry);
                }
            }
        }

        private bool UpdateCacheSizeExceedsCapacity(CacheEntry entry)
        {
            if (!_options.SizeLimit.HasValue)
            {
                return false;
            }

            long newSize = 0L;
            for (int i = 0; i < 100; i++)
            {
                long sizeRead = Interlocked.Read(ref _cacheSize);
                newSize = sizeRead + entry.Size.Value;

                if (newSize < 0 || newSize > _options.SizeLimit)
                {
                    // Overflow occurred, return true without updating the cache size
                    return true;
                }

                if (sizeRead == Interlocked.CompareExchange(ref _cacheSize, newSize, sizeRead))
                {
                    return false;
                }
            }

            return true;
        }

        private void TriggerOvercapacityCompaction()
        {
            _logger.LogDebug("Overcapacity compaction triggered");

            // Spawn background thread for compaction
            ThreadPool.QueueUserWorkItem(s => OvercapacityCompaction((MemoryCache)s), this);
        }

        private static void OvercapacityCompaction(MemoryCache cache)
        {
            long currentSize = Interlocked.Read(ref cache._cacheSize);

            cache._logger.LogDebug($"Overcapacity compaction executing. Current size {currentSize}");

            double? lowWatermark = cache._options.SizeLimit * (1 - cache._options.CompactionPercentage);
            if (currentSize > lowWatermark)
            {
                cache.Compact(currentSize - (long)lowWatermark, entry => entry.Size.Value);
            }

            cache._logger.LogDebug($"Overcapacity compaction executed. New size {Interlocked.Read(ref cache._cacheSize)}");
        }

        /// Remove at least the given percentage (0.10 for 10%) of the total entries (or estimated memory?), according to the following policy:
        /// 1. Remove all expired items.
        /// 2. Bucket by CacheItemPriority.
        /// 3. Least recently used objects.
        /// ?. Items with the soonest absolute expiration.
        /// ?. Items with the soonest sliding expiration.
        /// ?. Larger objects - estimated by object graph size, inaccurate.
        public void Compact(double percentage)
        {
            int removalCountTarget = (int)(_entries.Count * percentage);
            Compact(removalCountTarget, _ => 1);
        }

        private void Compact(long removalSizeTarget, Func<CacheEntry, long> computeEntrySize)
        {
            var entriesToRemove = new List<CacheEntry>();
            var lowPriEntries = new List<CacheEntry>();
            var normalPriEntries = new List<CacheEntry>();
            var highPriEntries = new List<CacheEntry>();
            long removedSize = 0;

            // Sort items by expired & priority status
            DateTimeOffset now = _options.Clock.UtcNow;
            foreach (KeyValuePair<object, CacheEntry> item in _entries)
            {
                CacheEntry entry = item.Value;
                if (entry.CheckExpired(now))
                {
                    entriesToRemove.Add(entry);
                    removedSize += computeEntrySize(entry);
                }
                else
                {
                    switch (entry.Priority)
                    {
                        case CacheItemPriority.Low:
                            lowPriEntries.Add(entry);
                            break;
                        case CacheItemPriority.Normal:
                            normalPriEntries.Add(entry);
                            break;
                        case CacheItemPriority.High:
                            highPriEntries.Add(entry);
                            break;
                        case CacheItemPriority.NeverRemove:
                            break;
                        default:
                            throw new NotSupportedException("Not implemented: " + entry.Priority);
                    }
                }
            }

            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, lowPriEntries);
            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, normalPriEntries);
            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, highPriEntries);

            foreach (CacheEntry entry in entriesToRemove)
            {
                RemoveEntry(entry);
            }

            // Policy:
            // 1. Least recently used objects.
            // ?. Items with the soonest absolute expiration.
            // ?. Items with the soonest sliding expiration.
            // ?. Larger objects - estimated by object graph size, inaccurate.
            static void ExpirePriorityBucket(ref long removedSize, long removalSizeTarget, Func<CacheEntry, long> computeEntrySize, List<CacheEntry> entriesToRemove,
                List<CacheEntry> priorityEntries)
            {
                // Do we meet our quota by just removing expired entries?
                if (removalSizeTarget <= removedSize)
                {
                    // No-op, we've met quota
                    return;
                }

                // Expire enough entries to reach our goal
                // TODO: Refine policy

                // LRU
                priorityEntries.Sort((e1, e2) => e1.LastAccessed.CompareTo(e2.LastAccessed));
                foreach (CacheEntry entry in priorityEntries)
                {
                    entry.SetExpired(EvictionReason.Capacity);
                    entriesToRemove.Add(entry);
                    removedSize += computeEntrySize(entry);

                    if (removalSizeTarget <= removedSize)
                    {
                        break;
                    }
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    GC.SuppressFinalize(this);
                }

                _disposed = true;
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                Throw();
            }

            static void Throw() => throw new ObjectDisposedException(typeof(MemoryCache).FullName);
        }

        private static void ValidateCacheKey(object key)
        {
            if (key == null)
            {
                Throw();
            }

            static void Throw() => throw new ArgumentNullException(nameof(key));
        }
    }

    internal sealed class CacheEntry : ICacheEntry
    {
        private static readonly Action<object> ExpirationCallback = ExpirationTokensExpired;

        private readonly MemoryCache _cache;

        private CacheEntryTokens _tokens; // might be null if user is not using the tokens or callbacks
        private TimeSpan? _absoluteExpirationRelativeToNow;
        private TimeSpan? _slidingExpiration;
        private long? _size;
        private object _value;
        private CacheEntryState _state;

        internal CacheEntry(object key, MemoryCache memoryCache)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            _cache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
            _state = new CacheEntryState(CacheItemPriority.Normal);
        }

        /// <summary>
        /// Gets or sets an absolute expiration date for the cache entry.
        /// </summary>
        public DateTimeOffset? AbsoluteExpiration { get; set; }

        /// <summary>
        /// Gets or sets an absolute expiration time, relative to now.
        /// </summary>
        public TimeSpan? AbsoluteExpirationRelativeToNow
        {
            get => _absoluteExpirationRelativeToNow;
            set
            {
                // this method does not set AbsoluteExpiration as it would require calling Clock.UtcNow twice:
                // once here and once in MemoryCache.SetEntry

                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(AbsoluteExpirationRelativeToNow),
                        value,
                        "The relative expiration value must be positive.");
                }

                _absoluteExpirationRelativeToNow = value;
            }
        }

        /// <summary>
        /// Gets or sets how long a cache entry can be inactive (e.g. not accessed) before it will be removed.
        /// This will not extend the entry lifetime beyond the absolute expiration (if set).
        /// </summary>
        public TimeSpan? SlidingExpiration
        {
            get => _slidingExpiration;
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(SlidingExpiration),
                        value,
                        "The sliding expiration value must be positive.");
                }

                _slidingExpiration = value;
            }
        }

        /// <summary>
        /// Gets the <see cref="IChangeToken"/> instances which cause the cache entry to expire.
        /// </summary>
        public IList<IChangeToken> ExpirationTokens => GetOrCreateTokens().ExpirationTokens;

        /// <summary>
        /// Gets or sets the callbacks will be fired after the cache entry is evicted from the cache.
        /// </summary>
        public IList<PostEvictionCallbackRegistration> PostEvictionCallbacks => GetOrCreateTokens().PostEvictionCallbacks;

        /// <summary>
        /// Gets or sets the priority for keeping the cache entry in the cache during a
        /// memory pressure triggered cleanup. The default is <see cref="CacheItemPriority.Normal"/>.
        /// </summary>
        public CacheItemPriority Priority { get => _state.Priority; set => _state.Priority = value; }

        /// <summary>
        /// Gets or sets the size of the cache entry value.
        /// </summary>
        public long? Size
        {
            get => _size;
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(value)} must be non-negative.");
                }

                _size = value;
            }
        }

        public object Key { get; private set; }

        public object Value
        {
            get => _value;
            set
            {
                _value = value;
                _state.IsValueSet = true;
            }
        }

        internal DateTimeOffset LastAccessed { get; set; }

        internal EvictionReason EvictionReason { get => _state.EvictionReason; private set => _state.EvictionReason = value; }

        public void Dispose()
        {
            if (!_state.IsDisposed)
            {
                _state.IsDisposed = true;

                // Don't commit or propagate options if the CacheEntry Value was never set.
                // We assume an exception occurred causing the caller to not set the Value successfully,
                // so don't use this entry.
                if (_state.IsValueSet)
                {
                    _cache.SetEntry(this);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)] // added based on profiling
        internal bool CheckExpired(in DateTimeOffset now)
            => _state.IsExpired
               || CheckForExpiredTime(now)
               || (_tokens != null && _tokens.CheckForExpiredTokens(this));

        internal void SetExpired(EvictionReason reason)
        {
            if (EvictionReason == EvictionReason.None)
            {
                EvictionReason = reason;
            }

            _state.IsExpired = true;
            _tokens?.DetachTokens();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)] // added based on profiling
        private bool CheckForExpiredTime(in DateTimeOffset now)
        {
            if (!AbsoluteExpiration.HasValue && !_slidingExpiration.HasValue)
            {
                return false;
            }

            return FullCheck(now);

            bool FullCheck(in DateTimeOffset offset)
            {
                if (AbsoluteExpiration.HasValue && AbsoluteExpiration.Value <= offset)
                {
                    SetExpired(EvictionReason.Expired);
                    return true;
                }

                if (_slidingExpiration.HasValue
                    && (offset - LastAccessed) >= _slidingExpiration)
                {
                    SetExpired(EvictionReason.Expired);
                    return true;
                }

                return false;
            }
        }

        internal void AttachTokens() => _tokens?.AttachTokens(this);

        private static void ExpirationTokensExpired(object obj)
        {
            // start a new thread to avoid issues with callbacks called from RegisterChangeCallback
            Task.Factory.StartNew(state =>
            {
                var entry = (CacheEntry)state;
                entry.SetExpired(EvictionReason.TokenExpired);
                entry._cache.EntryExpired(entry);
            }, obj, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        internal void InvokeEvictionCallbacks() => _tokens?.InvokeEvictionCallbacks(this);

        // this simple check very often allows us to avoid expensive call to PropagateOptions(CacheEntryHelper.Current)
        [MethodImpl(MethodImplOptions.AggressiveInlining)] // added based on profiling
        internal bool CanPropagateOptions() => (_tokens != null && _tokens.CanPropagateTokens()) || AbsoluteExpiration.HasValue;

        internal void PropagateOptions(CacheEntry parent)
        {
            if (parent == null)
            {
                return;
            }

            // Copy expiration tokens and AbsoluteExpiration to the cache entries hierarchy.
            // We do this regardless of it gets cached because the tokens are associated with the value we'll return.
            _tokens?.PropagateTokens(parent);

            if (AbsoluteExpiration.HasValue)
            {
                if (!parent.AbsoluteExpiration.HasValue || AbsoluteExpiration < parent.AbsoluteExpiration)
                {
                    parent.AbsoluteExpiration = AbsoluteExpiration;
                }
            }
        }

        private CacheEntryTokens GetOrCreateTokens()
        {
            if (_tokens != null)
            {
                return _tokens;
            }

            CacheEntryTokens result = new CacheEntryTokens();
            return Interlocked.CompareExchange(ref _tokens, result, null) ?? result;
        }

        // this type exists just to reduce average CacheEntry size
        // which typically is not using expiration tokens or callbacks
        private sealed class CacheEntryTokens
        {
            private List<IChangeToken> _expirationTokens;
            private List<IDisposable> _expirationTokenRegistrations;

            private List<PostEvictionCallbackRegistration>
                _postEvictionCallbacks; // this is not really related to tokens, but was moved here to shrink typical CacheEntry size

            internal List<IChangeToken> ExpirationTokens => _expirationTokens ??= new List<IChangeToken>();
            internal List<PostEvictionCallbackRegistration> PostEvictionCallbacks => _postEvictionCallbacks ??= new List<PostEvictionCallbackRegistration>();

            internal void AttachTokens(CacheEntry cacheEntry)
            {
                if (_expirationTokens != null)
                {
                    lock (this)
                    {
                        for (int i = 0; i < _expirationTokens.Count; i++)
                        {
                            IChangeToken expirationToken = _expirationTokens[i];
                            if (expirationToken.ActiveChangeCallbacks)
                            {
                                _expirationTokenRegistrations ??= new List<IDisposable>(1);
                                IDisposable registration = expirationToken.RegisterChangeCallback(ExpirationCallback, cacheEntry);
                                _expirationTokenRegistrations.Add(registration);
                            }
                        }
                    }
                }
            }

            internal bool CheckForExpiredTokens(CacheEntry cacheEntry)
            {
                if (_expirationTokens != null)
                {
                    for (int i = 0; i < _expirationTokens.Count; i++)
                    {
                        IChangeToken expiredToken = _expirationTokens[i];
                        if (expiredToken.HasChanged)
                        {
                            cacheEntry.SetExpired(EvictionReason.TokenExpired);
                            return true;
                        }
                    }
                }

                return false;
            }

            internal bool CanPropagateTokens() => _expirationTokens != null;

            internal void PropagateTokens(CacheEntry parentEntry)
            {
                if (_expirationTokens != null)
                {
                    lock (this)
                    {
                        lock (parentEntry.GetOrCreateTokens())
                        {
                            foreach (IChangeToken expirationToken in _expirationTokens)
                            {
                                parentEntry.AddExpirationToken(expirationToken);
                            }
                        }
                    }
                }
            }

            internal void DetachTokens()
            {
                // _expirationTokenRegistrations is not checked for null, because AttachTokens might initialize it under lock
                // instead we are checking for _expirationTokens, because if they are not null, then _expirationTokenRegistrations might also be not null
                if (_expirationTokens != null)
                {
                    lock (this)
                    {
                        List<IDisposable> registrations = _expirationTokenRegistrations;
                        if (registrations != null)
                        {
                            _expirationTokenRegistrations = null;
                            for (int i = 0; i < registrations.Count; i++)
                            {
                                IDisposable registration = registrations[i];
                                registration.Dispose();
                            }
                        }
                    }
                }
            }

            internal void InvokeEvictionCallbacks(CacheEntry cacheEntry)
            {
                if (_postEvictionCallbacks != null)
                {
                    Task.Factory.StartNew(state => InvokeCallbacks((CacheEntry)state), cacheEntry,
                        CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                }
            }

            private static void InvokeCallbacks(CacheEntry entry)
            {
                List<PostEvictionCallbackRegistration> callbackRegistrations = Interlocked.Exchange(ref entry._tokens._postEvictionCallbacks, null);

                if (callbackRegistrations == null)
                {
                    return;
                }

                for (int i = 0; i < callbackRegistrations.Count; i++)
                {
                    PostEvictionCallbackRegistration registration = callbackRegistrations[i];

                    try
                    {
                        registration.EvictionCallback?.Invoke(entry.Key, entry.Value, entry.EvictionReason, registration.State);
                    }
                    catch (Exception e)
                    {
                        // This will be invoked on a background thread, don't let it throw.
                        entry._cache._logger.LogError(e, "EvictionCallback invoked failed");
                    }
                }
            }
        }

        // this type exists just to reduce CacheEntry size by replacing many enum & boolean fields with one of a size of Int32
        private struct CacheEntryState
        {
            private byte _flags;
            private byte _evictionReason;
            private byte _priority;

            internal CacheEntryState(CacheItemPriority priority) : this() => _priority = (byte)priority;

            internal bool IsDisposed
            {
                get => ((Flags)_flags & Flags.IsDisposed) != 0;
                set => SetFlag(Flags.IsDisposed, value);
            }

            internal bool IsExpired
            {
                get => ((Flags)_flags & Flags.IsExpired) != 0;
                set => SetFlag(Flags.IsExpired, value);
            }

            internal bool IsValueSet
            {
                get => ((Flags)_flags & Flags.IsValueSet) != 0;
                set => SetFlag(Flags.IsValueSet, value);
            }

            internal EvictionReason EvictionReason
            {
                get => (EvictionReason)_evictionReason;
                set => _evictionReason = (byte)value;
            }

            internal CacheItemPriority Priority
            {
                get => (CacheItemPriority)_priority;
                set => _priority = (byte)value;
            }

            private void SetFlag(Flags option, bool value) => _flags = (byte)(value ? (_flags | (byte)option) : (_flags & ~(byte)option));

            [Flags]
            private enum Flags : byte
            {
                Default = 0,
                IsValueSet = 1 << 0,
                IsExpired = 1 << 1,
                IsDisposed = 1 << 2,
            }
        }
    }
}
