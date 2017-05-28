using System;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private readonly ThreadLocal<StackHeader> _contextPool;
        private readonly Timer _timer;
        private bool _disposed;
        protected LowMemoryFlag LowMemoryFlag = new LowMemoryFlag();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private class StackHeader : IDisposable
        {
            public StackNode Head;

            ~StackHeader()
            {
                Dispose();
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);

                var current = Head;
                while (current != null)
                {
                    var ctx = current.Value;
                    current = current.Next;
                    if (ctx == null)
                        continue;
                    if (Interlocked.CompareExchange(ref ctx.InUse, 1, 0) != 0)
                        continue;
                    ctx.Dispose();
                }
            }
        }

        private class StackNode
        {
            public T Value;
            public StackNode Next;
        }

        protected JsonContextPoolBase()
        {
            _contextPool = new ThreadLocal<StackHeader>(() => new StackHeader(), trackAllValues: true);
            _timer = new Timer(CleanNativeMemoryTimer, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        private void CleanNativeMemoryTimer(object state)
        {
            if (_disposed)
                return;

            var lockTaken = false;
            try
            {
                Monitor.TryEnter(this, ref lockTaken);
                if (lockTaken == false)
                    return;

                if (_disposed)
                    return;

                var now = DateTime.UtcNow;
                foreach (var header in _contextPool.Values)
                {
                    var current = header.Head;
                    while (current != null)
                    {
                        var ctx = current.Value;
                        var parent = current;
                        current = current.Next;

                        if (ctx == null)
                            continue;

                        if (LowMemoryFlag.LowMemoryState == 0)
                        {
                            var timeInPool = now - ctx.InPoolSince;
                            if (timeInPool < TimeSpan.FromMinutes(1))
                                continue;
                        } // else dispose context on low mem stress

                        // it is too old, we can dispose it, but need to protect from races
                        // if the owner thread will just pick it up

                        if (Interlocked.CompareExchange(ref ctx.InUse, 1, 0) != 0)
                            continue;

                        ctx.Dispose();
                        parent.Value = null;
                    }
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(this);
            }
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            T ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;

            return disposable;
        }

        public void Clean()
        {
            // we are expecting to be called here when there is no
            // more work to be done, and we want to release resources
            // to the system

            var stack = _contextPool.Value;
            var current = Interlocked.Exchange(ref stack.Head, null);
            while (current != null)
            {
                current.Value?.Dispose();
                current = current.Next;
            }
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var currentThread = _contextPool.Value;
            IDisposable returnContext;
            if (TryReuseExistingContextFrom(currentThread, out context, out returnContext))
                return returnContext;

            // couldn't find it on our own thread, let us try and steal from other threads
            foreach (var otherThread in _contextPool.Values)
            {
                if (otherThread == currentThread)
                    continue;
                if (TryReuseExistingContextFrom(otherThread, out context, out returnContext))
                    return returnContext;
            }

            // no choice, got to create it
            context = CreateContext();
            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        private bool TryReuseExistingContextFrom(StackHeader stack, out T context, out IDisposable disposable)
        {
            while (true)
            {
                var current = stack.Head;
                if (current == null)
                    break;
                if (Interlocked.CompareExchange(ref stack.Head, current.Next, current) != current)
                    continue;
                context = current.Value;
                if (context == null)
                    continue;
                if (Interlocked.CompareExchange(ref context.InUse, 1, 0) != 0)
                    continue;
                context.Renew();
                disposable = new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
                return true;
            }

            context = default(T);
            disposable = null;
            return false;
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                Context.Reset();
                Interlocked.Exchange(ref Context.InUse, 0);
                Context.InPoolSince = DateTime.UtcNow;

                Parent.Push(Context);
            }

        }

        private void Push(T context)
        {
            StackHeader threadHeader;
            try
            {
                threadHeader = _contextPool.Value;
            }
            catch (ObjectDisposedException)
            {
                context.Dispose();
                return;
            }
            while (true)
            {
                var current = threadHeader.Head;
                var newHead = new StackNode { Value = context, Next = current };
                if (Interlocked.CompareExchange(ref threadHeader.Head, newHead, current) == current)
                    return;
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            lock (this)
            {
                if (_disposed)
                    return;
                _cts.Cancel();
                _disposed = true;
                _timer.Dispose();
                foreach (var stack in _contextPool.Values)
                {
                    stack.Dispose();
                }
                _contextPool.Dispose();
            }
        }

        public void LowMemory()
        {
            if (Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 1, 0) != 0)
                return;
            CleanNativeMemoryTimer(null);
        }

        public void LowMemoryOver()
        {
            Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 0, 1);
        }
    }
}