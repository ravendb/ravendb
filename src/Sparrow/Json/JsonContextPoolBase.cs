using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T>
        where T : JsonOperationContext
    {
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private readonly ThreadLocal<MutliReaderSingleWriterStack> _contextPool = new ThreadLocal<MutliReaderSingleWriterStack>(() => new MutliReaderSingleWriterStack(), trackAllValues: true);
        private readonly Timer _timer;
        private bool _disposed;

        /// <summary>
        /// This class is meant to be read from the timer callback
        /// It is safe to read from another thread, although you may
        /// get a stale view of the data
        /// </summary>
        private class MutliReaderSingleWriterStack : IEnumerable<T>
        {
            private T _fastPath;
            private T[] _stack;
            private int _stackUsage;
            public int Count;

            public T Pop()
            {
                if (Count == 0)
                    throw new InvalidOperationException("Attempt to pop an empty stack");

                Count--;
                if (_fastPath != null)
                {
                    var ctx = _fastPath;
                    _fastPath = null;
                    return ctx;
                }
                return _stack[--_stackUsage];
            }

            public void Push(T context)
            {
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
                _stackUsage = 0;
                _fastPath = null;
                Count = 0;
            }

            public IEnumerator<T> GetEnumerator()
            {
                var ctx = _fastPath;
                if (ctx != null)
                    yield return ctx;

                // we assume that the code is racy, stale data is fine here
                var array = _stack;
                if(array == null)
                    yield break;
                var len = Math.Min(_stackUsage, array.Length);

                for (int i = len - 1; i >= 0; i--)
                {
                    ctx = array[i];
                    if (ctx != null)
                        yield return ctx;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public JsonContextPoolBase()
        {
            _timer = new Timer(TimerCallback, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

        private void TimerCallback(object state)
        {
            if (_disposed)
                return;
            var now = DateTime.UtcNow;
            foreach (var threadPool in _contextPool.Values)
            {
                foreach (var ctx in threadPool)
                {
                    // note that this is a racy call, need to be careful here
                    if (ctx == null)
                        continue;

                    var timeInPool = now - ctx.InPoolSince;
                    if (timeInPool < TimeSpan.FromMinutes(1))
                        continue;

                    // it is too old, we can dispose it, but need to protect from races
                    // if the owner thread will just pick it up

                    if (Interlocked.CompareExchange(ref ctx.InUse, 1, 0) != 0)
                        continue;

                    ctx.Dispose();
                }
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
            foreach (var item in stack)
            {
                item.Dispose();
            }
            stack.Clear();

        }

        public IDisposable AllocateOperationContext(out T context)
        {
            var stack = _contextPool.Value;
            while (stack.Count > 0)
            {
                context = stack.Pop();
                if (Interlocked.CompareExchange(ref context.InUse, 1, 0) != 0)
                    continue;
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
            _disposed = true;
            _timer.Dispose();
            foreach (var stack in _contextPool.Values)
            {
                while (stack.Count > 0)
                {
                    var ctx = stack.Pop();
                    ctx.Dispose();
                }
            }
            _contextPool.Dispose();
        }
    }
}