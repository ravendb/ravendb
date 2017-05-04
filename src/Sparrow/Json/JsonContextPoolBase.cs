using System;
using System.Threading;
using Sparrow.LowMemory;

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
        private readonly ThreadLocal<MutliReaderSingleWriterStack> _contextPool;
        private readonly Timer _timer;
        private bool _disposed;
        protected LowMemoryFlag LowMemoryFlag = new LowMemoryFlag();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        /// <summary>
        /// This class is meant to be read from the timer callback
        /// It is safe to read from another thread, although you may
        /// get a stale view of the data
        /// </summary>
        private class MutliReaderSingleWriterStack 
        {
            private CancellationToken _token;
            private T _fastPath;
            private T[] _stack;
            private int _stackUsage;
            public int Count;

            public MutliReaderSingleWriterStack(CancellationToken token)
            {
                _token = token;
            }

            public T Pop()
            {
                _token.ThrowIfCancellationRequested();
                if (Count == 0)
                    ThrowEmptyStack();

                Count--;
                if (_fastPath != null)
                {
                    var ctx = _fastPath;
                    _fastPath = null;
                    return ctx;
                }
                return _stack[--_stackUsage];
            }

            private static void ThrowEmptyStack()
            {
                throw new InvalidOperationException("Attempt to pop an empty stack");
            }

            public void Push(T context)
            {
                if (_token.IsCancellationRequested)
                {
                    context.Dispose();
                    return;
                }
                Count++;
                if (Count == 1)
                {
                    _fastPath = context;
                    return;
                }
                if (_stack == null)
                {
                    _stack = new T[4];
                    _stack[0] = context;
                    _stackUsage = 1;

                    return;
                }

                if (_stackUsage >= _stack.Length)
                {
                    var old = _stack;
                    _stack = new T[old.Length * 2];
                    Array.Copy(old, _stack, old.Length);
                }
                _stack[_stackUsage++] = context;
            }


            public void Clear()
            {
                _token.ThrowIfCancellationRequested();
                _stackUsage = 0;
                _fastPath = null;
                Count = 0;
            }

            public ArraySegment<T> Snapshot 
            {
                get
                {
                    var array = _stack;

                    if(array == null)
                        return new ArraySegment<T>(Array.Empty<T>());

                    var min = Math.Min(_stackUsage, array.Length);

                    return new ArraySegment<T>(array, 0, min);
                }
            }
        }

        protected JsonContextPoolBase()
        {
            _contextPool = new ThreadLocal<MutliReaderSingleWriterStack>(() => new MutliReaderSingleWriterStack(_cts.Token), trackAllValues: true);
            _timer = new Timer(CleanNativeMemoryTimer, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        private void CleanNativeMemoryTimer(object state)
        {
            if (_disposed)
                return;
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(this, ref lockTaken);
                if (lockTaken == false)
                    return;

                if (_disposed)
                    return;

                var now = DateTime.UtcNow;
                foreach (var treahdStack in _contextPool.Values)
                {
                    var items = treahdStack.Snapshot;
                    for (int i = items.Offset; i < items.Count; i++)
                    {
                        var ctx = items.Array[i];
                        // note that this is a racy call, need to be careful here
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

                        items.Array[i] = null;
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

            if (stack.Count == 0)
                return; // nothing to do;
            var items = stack.Snapshot;
            for (int i = items.Offset; i < items.Count; i++)
            {
                items.Array[i].Dispose();
            }
            stack.Clear();

        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var stack = _contextPool.Value;
            while (stack.Count > 0)
            {
                context = stack.Pop();
                if (context == null)
                    continue;
                if (Interlocked.CompareExchange(ref context.InUse, 1, 0) != 0)
                    continue;
                context.Renew();
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            context = CreateContext();
            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                var stack = GetCurrentThreadStack();
                if (stack == null)
                {
                    Context.Dispose();
                    return;
                }
                Context.Reset();
                Interlocked.Exchange(ref Context.InUse, 0);
                Context.InPoolSince = DateTime.UtcNow;
                stack.Push(Context);
            }

            private MutliReaderSingleWriterStack GetCurrentThreadStack()
            {
                try
                {
                    return Parent._contextPool.Value;
                }
                catch (ObjectDisposedException)
                {
                    return null;
                }
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
                    var items = stack.Snapshot;
                    for (int i = items.Offset; i < items.Count; i++)
                    {
                        var ctx = items.Array[i];
                        if(ctx == null)
                            continue;
                        if (Interlocked.CompareExchange(ref ctx.InUse, 1, 0) != 0)
                            continue;
                        ctx.Dispose();
                    }
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