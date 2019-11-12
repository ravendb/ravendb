using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        private ConcurrentDictionary<int, ContextStack> _contextStacksByThreadId;
        private readonly StrongReference<ConcurrentDictionary<int, ContextStack>> _contextStacksByThreadIdStrongRef;
        private NativeMemoryCleaner<ContextStack, T> _nativeMemoryCleaner;
        private bool _disposed;
        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        // because this is a finalizer object, we want to pool them to avoid having too many items in the finalization queue
        private static readonly ObjectPool<ContextStack> _contextStackPool = new ObjectPool<ContextStack>(() => new ContextStack());

        [ThreadStatic]
        private static ContextStackThreadReleaser _releaser;

        private class ContextStackThreadReleaser
        {
            private readonly int _ownerThread;
            private readonly StrongReference<ConcurrentDictionary<int, ContextStack>> _contextStacksByThreadId;

            public ContextStackThreadReleaser(int ownerThread, StrongReference<ConcurrentDictionary<int, ContextStack>> contextStacksByThreadId)
            {
                _ownerThread = ownerThread;
                _contextStacksByThreadId = contextStacksByThreadId;
            }

            ~ContextStackThreadReleaser()
            {
                var current = _contextStacksByThreadId.Value;
                if (current != null) // may have already been finalized
                    current.TryRemove(_ownerThread, out _);
            }
        }

        private void EnsureCurrentThreadContextWillBeReleased(int currentThreadId)
        {
            if (_releaser == null)
            {
                var contextStackThreadReleaser = new ContextStackThreadReleaser(currentThreadId, _contextStacksByThreadIdStrongRef);
                _releaser = contextStackThreadReleaser;
            }
        }

        private class ContextStack : StackHeader<T>, IDisposable
        {
            public bool AvoidWorkStealing;
            public void Dispose()
            {
                GC.SuppressFinalize(this);
                DisposeOfContexts();
                // explicitly don't want to do this in the finalizer, if the instance leaked, so be it
                _contextStackPool.Free(this);
            }

            private void DisposeOfContexts()
            {
                var current = Interlocked.Exchange(ref Head, null);

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
            _contextStacksByThreadId = new ConcurrentDictionary<int, ContextStack>();
            _contextStacksByThreadIdStrongRef = new StrongReference<ConcurrentDictionary<int, ContextStack>>
            {
                Value = _contextStacksByThreadId
            };

            ThreadLocalCleanup.ReleaseThreadLocalState += CleanThreadLocalState;
            _nativeMemoryCleaner = new NativeMemoryCleaner<ContextStack, T>(this, s => ((JsonContextPoolBase<T>)s).EnumerateAllThreadContexts().ToList(),
                LowMemoryFlag, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        private ContextStack GetCurrentContextStack()
        {
            var threadId = _contextStacksByThreadId?.GetOrAdd(NativeMemory.CurrentThreadStats.InternalId,
                currentThreadId =>
                {
                    EnsureCurrentThreadContextWillBeReleased(currentThreadId);
                    var ctx = _contextStackPool.Allocate();
                    ctx.AvoidWorkStealing = JsonContextPoolWorkStealing.AvoidForCurrentThread;
                    return ctx;
                });
            return threadId;
        }

        private void CleanThreadLocalState()
        {
            try
            {
                if (_contextStacksByThreadId != null &&
                    _contextStacksByThreadId.TryRemove(NativeMemory.CurrentThreadStats.InternalId, out var stack))
                {

                    var current = Interlocked.Exchange(ref stack.Head, ContextStack.HeaderDisposed);
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
            }
            catch (ObjectDisposedException)
            {
                // nothing to do here
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
            var stack = GetCurrentContextStack();
            var current = Interlocked.Exchange(ref stack.Head, null);
            while (current != null)
            {
                current.Value?.Dispose();
                current = current.Next;
            }
        }

        private IEnumerable<ContextStack> EnumerateAllThreadContexts()
        {
            foreach (var contextStack in _contextStacksByThreadId)
            {
                yield return contextStack.Value;
            }
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var currentThread = GetCurrentContextStack();
            if (TryReuseExistingContextFrom(currentThread, out context, out IDisposable returnContext))
                return returnContext;

            // couldn't find it on our own thread, let us try and steal from other threads

            if (currentThread.AvoidWorkStealing == false)
            {
                foreach (var otherThread in EnumerateAllThreadContexts())
                {
                    if (otherThread == currentThread || otherThread.AvoidWorkStealing)
                        continue;
                    if (TryReuseExistingContextFrom(otherThread, out context, out returnContext))
                        return returnContext;
                }
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

            context = default;
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
                threadHeader = GetCurrentContextStack();
                if (threadHeader == null) // the parent was already disposed
                    return;
            }
            catch (ObjectDisposedException)
            {
                context.Dispose();
                return;
            }
            while (true)
            {
                var current = threadHeader.Head;
                if (current == ContextStack.HeaderDisposed)
                {
                    context.Dispose();
                    return;
                }
                var newHead = new StackNode<T> { Value = context, Next = current };
                if (Interlocked.CompareExchange(ref threadHeader.Head, newHead, current) == current)
                    return;
            }
        }
        public virtual void Dispose()
        {
            if (_disposed)
                return;
            lock (this)
            {
                if (_disposed)
                    return;

                _cts.Cancel();
                _disposed = true;
                CleanThreadLocalState();
                ThreadLocalCleanup.ReleaseThreadLocalState -= CleanThreadLocalState;
                _nativeMemoryCleaner.Dispose();
                _nativeMemoryCleaner = null;

                foreach (var kvp in EnumerateAllThreadContexts())
                {
                    kvp.Dispose();
                }
                _contextStacksByThreadId?.Clear();
                _contextStacksByThreadId = null;
                _contextStacksByThreadIdStrongRef.Value = null;
            }
        }

        public void LowMemory()
        {
            if (LowMemoryFlag.Raise())
                _nativeMemoryCleaner?.CleanNativeMemory(null);
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
        }
    }

    public static class JsonContextPoolWorkStealing
    {
        [ThreadStatic]
        public static bool AvoidForCurrentThread;
    }

}
