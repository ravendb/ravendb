using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Threading;

namespace Sparrow.Utils
{

    /// <summary>
    /// The NativeMemoryCleaner is responsible for periodically scanning a lock-free ring buffer of pooled items and
    /// disposing of items that are either no longer needed or were allocated under low memory pressure.
    /// 
    /// It uses a SharedMultipleUseFlag (_lowMemoryFlag) and an idle time threshold (_idleTime) to decide which items
    /// should be disposed. Items are dequeued from the buffer and checked:
    /// - If we are under low memory conditions (LowMemoryFlag is raised) or the item has been idle longer than _idleTime, 
    ///   the item is disposed.
    /// - Otherwise, we attempt to return the item to the pool.
    /// 
    /// Because the LockFreeRingBuffer is thread-safe, we do not use a global lock. Instead, we optimistically dequeue items 
    /// and handle them. If the queue is nearly empty (less than MinimumRetainedItemsInQueue), we stop cleaning to ensure some items
    /// remain available for immediate allocation.
    /// 
    /// This approach reduces contention and overhead, improving throughput in memory-intensive operations.
    /// </summary>
    /// <param name="lowMemoryFlag">
    /// A flag that indicates whether the system is under low memory pressure.
    /// When raised, the cleaner becomes more aggressive in disposing of items.
    /// </param>
    /// <param name="idleTime">
    /// The maximum duration an item can remain idle in the pool before it is considered for disposal.
    /// </param>
    public sealed class NativeMemoryCleaner<TPooledItem> : IDisposable
        where TPooledItem : PooledItem
    {
        private static readonly IRavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrow(typeof(NativeMemoryCleaner<TPooledItem>));

        private readonly LockFreeRingBuffer<TPooledItem> _ringBuffer;
        private readonly SharedMultipleUseFlag _lowMemoryFlag;
        private readonly TimeSpan _idleTime;
        private readonly Timer _timer;

        private bool _disposed;

        public NativeMemoryCleaner(LockFreeRingBuffer<TPooledItem> ringBuffer, SharedMultipleUseFlag lowMemoryFlag, TimeSpan period, TimeSpan idleTime)
        {
            _ringBuffer = ringBuffer ?? throw new ArgumentNullException(nameof(ringBuffer));
            _lowMemoryFlag = lowMemoryFlag ?? throw new ArgumentNullException(nameof(lowMemoryFlag));
            _idleTime = idleTime;
            _timer = new Timer(x => PurgeStaleOrExcessItems(), null, period, period);
        }

        private const int MinimumRetainedItemsInQueue = 2;

        /// <summary>
        /// Called periodically by the timer to clean up old or unneeded items.
        /// This method no longer takes a global lock, as the ring buffer is thread-safe.
        /// We aggressively and optimistically dequeue items, check their conditions, and either dispose or re-enqueue them.
        /// </summary>
        public void PurgeStaleOrExcessItems()
        {
            if (_disposed)
                return;

            // Errors during purging (e.g., ObjectDisposedException) are expected and ignored,
            // ensuring the cleaner does not disrupt the main allocation flow.

            try
            {
                int maxIterations = _ringBuffer.Count;

                // Exit early if the pool has too few items to justify cleanup.
                if (maxIterations <= MinimumRetainedItemsInQueue)
                    return; 

                DateTime now = DateTime.UtcNow;
                for (int i = 0; i < maxIterations; i++)
                {
                    if (_ringBuffer.TryDequeue(out var item) == false)
                        break; // Failed to dequeue, move on.

                    if (item == null)
                        continue; // Null items shouldn't happen, but we guard against it.

                    Debug.Assert(item.InUse.IsRaised() == false, "Item should not be in use when in the pool.");
                    
                    // Check if the item has been idle for too long or if we are under memory pressure.
                    var timeInPool = now - item.InPoolSince;
                    bool shouldDispose = _lowMemoryFlag.IsRaised() || timeInPool >= _idleTime;
                    if (shouldDispose == false)
                    {
                        // Item is still valid, and we are not under memory pressure, attempt to return it back to the pool
                        if (_ringBuffer.TryEnqueue(item) == false)
                        {
                            try
                            {
                                // Fallback: if the pool is full, dispose to prevent memory growth.
                                item.Dispose();
                            }
                            catch (ObjectDisposedException)
                            {
                                // Item was already disposed elsewhere; ignore and continue.
                            }
                        }

                        // If the pool now has too few items, stop cleaning.
                        if (_ringBuffer.Count <= MinimumRetainedItemsInQueue)
                            break;

                        continue; // Move on to the next item.
                    }

                    // Dispose of the item because it is stale or memory pressure is high.

                    // Ensure no other thread is using it and skip if we cannot safely raise InUse.
                    // This shouldn't happen, but we guard against it.
                    if (item.InUse.Raise() == false)
                        continue; 

                    try
                    {
                        item.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Item was already disposed elsewhere; ignore and continue.
                    }
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (Logger.IsErrorEnabled)
                    Logger.Error("Error during cleanup.", e);
            }
        }

        public void Dispose()
        {
#if !NETSTANDARD1_3
            using (var waitHandle = new ManualResetEvent(false))
            {
                _disposed = true;
                if (_timer.Dispose(waitHandle))
                {
                    waitHandle.WaitOne();
                }
            }
#else
            lock (_lock) // prevent from running the callback _after_ dispose
            {
                _disposed = true;
                _timer.Dispose();
            }
#endif
        }
    }
}
