using System;
using System.Threading;
using Sparrow.LowMemory;

namespace Sparrow.Utils
{
    public sealed class NativeMemoryCleaner<TStack, TPooledItem> : IDisposable where TPooledItem : PooledItem where TStack : StackHeader<TPooledItem>
    {
        private readonly ThreadLocal<TStack> _pool;
        private readonly object _lock = new object();
        private readonly LowMemoryFlag _lowMemoryFlag;
        private readonly TimeSpan _idleTime;
        private readonly Timer _timer;

        public NativeMemoryCleaner(ThreadLocal<TStack> pool, LowMemoryFlag lowMemoryFlag, TimeSpan period, TimeSpan idleTime)
        {
            _pool = pool;
            _lowMemoryFlag = lowMemoryFlag;
            _idleTime = idleTime;
            _timer = new Timer(CleanNativeMemory, null, period, period);
        }

        public void CleanNativeMemory(object state)
        {
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(_lock, ref lockTaken);
                if (lockTaken == false)
                    return;

                var now = DateTime.UtcNow;
                foreach (var header in _pool.Values)
                {
                    var current = header.Head;
                    while (current != null)
                    {
                        var item = current.Value;
                        var parent = current;
                        current = current.Next;

                        if (item == null)
                            continue;

                        if (_lowMemoryFlag.LowMemoryState == 0)
                        {
                            var timeInPool = now - item.InPoolSince;
                            if (timeInPool < _idleTime)
                                continue;
                        } // else dispose context on low mem stress

                        // it is too old, we can dispose it, but need to protect from races
                        // if the owner thread will just pick it up

                        if (Interlocked.CompareExchange(ref item.InUse, 1, 0) != 0)
                            continue;

                        try
                        {
                            item.Dispose();
                        }
                        catch (ObjectDisposedException)
                        {
                            // it is possible that this has already been diposed
                        }

                        parent.Value = null;
                    }
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_lock);
            }
        }

        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}