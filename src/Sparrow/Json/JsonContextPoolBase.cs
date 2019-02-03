using System;
using System.Buffers;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Threading;
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
        private readonly ThreadLocal<ContextStack> _contextPool;
        private readonly NativeMemoryCleaner<ContextStack, T> _nativeMemoryCleaner;
        private bool _disposed;
        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        // because this is a finalizer object, we want to pool them to avoid having too many items in the finalization queue
        private static ObjectPool<ContextStack> _contextStackPool = new ObjectPool<ContextStack>(() => new ContextStack());

        private class ContextStack : StackHeader<T>, IDisposable
        {
            public void Dispose()
            {
                GC.SuppressFinalize(this);
                DisposeOfContexts();
                // explicitly don't want to do this in the finalizer, if the instance leaked, so be it
                _contextStackPool.Free(this);
            }

            private void DisposeOfContexts()
            {
                var current = Interlocked.Exchange(ref Head, HeaderDisposed);
                while (current != null)
                {
                    var ctx = current.Value;
                    current = current.Next;
                    if (ctx == null)
                        continue;
                    if (!ctx.InUse.Raise())
                        continue;
                    ctx.Dispose();
                }
            }
        }

        protected JsonContextPoolBase()
        {
            _contextPool = new ThreadLocal<ContextStack>(() => _contextStackPool.Allocate(), trackAllValues: true);
            ThreadLocalCleanup.ReleaseThreadLocalState += CleanThreadLocalState;
            _nativeMemoryCleaner = new NativeMemoryCleaner<ContextStack, T>(_contextPool, LowMemoryFlag, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        private void CleanThreadLocalState()
        {
            StackNode<T> current;
            try
            {
                if (_contextPool.IsValueCreated == false)
                    return;

                current = _contextPool.Value?.Head;

                if (current == null)
                    return;

                _contextPool.Value.Head = null;
            }
            catch (ObjectDisposedException)
            {
                return; // the context pool was already disposed
            }

            while (current != null)
            {
                var value = current.Value;

                if (value != null)
                {
                    if (value.InUse.Raise()) // it could be stolen by another thread - RavenDB-11409
                        value.Dispose();
                }

                current = current.Next;
            }
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            var disposable = AllocateOperationContext(out T ctx);
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
            if (TryReuseExistingContextFrom(currentThread, out context, out IDisposable returnContext))
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

        private bool TryReuseExistingContextFrom(ContextStack stack, out T context, out IDisposable disposable)
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
                if (!context.InUse.Raise())
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
                if (Parent == null)
                    return;// disposed already

                if (Context.DoNotReuse)
                {
                    Context.Dispose();
                    return;
                }

                Context.Reset();
                // These contexts are reused, so we don't want to use LowerOrDie here.
                Context.InUse.Lower();
                Context.InPoolSince = DateTime.UtcNow;

                Parent.Push(Context);

                Parent = null;
                Context = null;
            }

        }

        private void Push(T context)
        {
            ContextStack threadHeader;
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
                if(current == ContextStack.HeaderDisposed)
                {
                    context.Dispose();
                    return;
                }
                var newHead = new StackNode<T> { Value = context, Next = current };
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
                ThreadLocalCleanup.ReleaseThreadLocalState -= CleanThreadLocalState;
                _nativeMemoryCleaner.Dispose();
                foreach (var stack in _contextPool.Values)
                {
                    stack.Dispose();
                }
                _contextPool.Dispose();
            }
        }

        public void LowMemory()
        {
            if (LowMemoryFlag.Raise())
                _nativeMemoryCleaner.CleanNativeMemory(null);
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
        }
    }
}
