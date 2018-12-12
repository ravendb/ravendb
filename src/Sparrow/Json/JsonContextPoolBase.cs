using System;
using System.Buffers;
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
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private ConcurrentDictionary<int, ContextStack> _contextStacksByThreadId = new ConcurrentDictionary<int, ContextStack>();
        private ThreadIdHolder[] _threadIds = Array.Empty<ThreadIdHolder>();

        private class ThreadIdHolder
        {
            public int ThreadId;
        }

        private readonly NativeMemoryCleaner<ContextStack, T> _nativeMemoryCleaner;
        private bool _disposed;
        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        // because this is a finalizer object, we want to pool them to avoid having too many items in the finalization queue
        private static ObjectPool<ContextStack> _contextStackPool = new ObjectPool<ContextStack>(() => new ContextStack());

        private class ContextStackThreadReleaser
        {
            private readonly JsonContextPoolBase<T> _parent;
            int _threadId;
            public ThreadIdHolder ThreadIdHolder;

            public ContextStackThreadReleaser(JsonContextPoolBase<T> parent)
            {
                _threadId = NativeMemory.CurrentThreadStats.Id;
                ThreadIdHolder = new ThreadIdHolder
                {
                    ThreadId = _threadId
                };
                _parent = parent;
            }

            ~ContextStackThreadReleaser()
            {
                ThreadIdHolder.ThreadId = -1;
                if (_parent._contextStacksByThreadId.TryRemove(_threadId, out var contextStack))
                {
                    // We expect this to be happen only when we lose a thread pool thread
                    // which can usually only happen on long idle periods. Therefor, we 
                    // want to dispose the memory for this thread.
                    while (contextStack.Head != null)
                    {
                        if (contextStack.Head.Value.InUse.Raise())
                        {
                            contextStack.Head.Value.Dispose();
                        }
                        contextStack.Head = contextStack.Head.Next;
                    }
                }
            }
        }

        [ThreadStatic]
        private static ContextStackThreadReleaser _releaser;
        

        private void EnsureCurrentThreadContextWillBeReleased()
        {
            if (_releaser != null)
                return;
            _releaser = new ContextStackThreadReleaser(this);

            while (true)
            {
                var currentThreadId = NativeMemory.CurrentThreadStats.Id;
                var copy = _threadIds;
                for (int i = 0; i < copy.Length; i++)
                {
                    if (copy[i].ThreadId == -1)
                    {
                        if(Interlocked.CompareExchange(ref copy[i].ThreadId, currentThreadId, -1) == -1)
                        {
                            _releaser.ThreadIdHolder = copy[i];
                            return;
                        }
                        return;
                    }
                }

                var threads = new ThreadIdHolder[copy.Length + 1];
                Array.Copy(copy, threads, copy.Length);
                threads[copy.Length] = _releaser.ThreadIdHolder;
                if (Interlocked.CompareExchange(ref _threadIds, threads, copy) == copy)
                    break;
            }
        }
  
        private class ContextStack : StackHeader<T>, IDisposable
        {
            public bool AvoidWorkStealing;

            ~ContextStack()
            {
                if (Environment.HasShutdownStarted)
                    return; // let the OS clean this up

                try
                {
                    DisposeOfContexts();
                }
                catch (ObjectDisposedException)
                {
                    // This is expected, we might be calling the finalizer on an object that
                    // was already disposed, we don't want to error here because of this
                }
            }

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
            ThreadLocalCleanup.ReleaseThreadLocalState += CleanThreadLocalState;
            _nativeMemoryCleaner = new NativeMemoryCleaner<ContextStack, T>(()=> EnumerateAllThreadContexts().ToList(),
                LowMemoryFlag, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        private ContextStack MaybeGetCurrentContextStack()
        {
            _contextStacksByThreadId.TryGetValue(NativeMemory.CurrentThreadStats.Id, out var x);
            return x;
        }

        private ContextStack GetCurrentContextStack()
        {
            return _contextStacksByThreadId.GetOrAdd(NativeMemory.CurrentThreadStats.Id, 
                _=>
                {
                    EnsureCurrentThreadContextWillBeReleased();
                    var ctx =  _contextStackPool.Allocate();
                    ctx.AvoidWorkStealing = JsonContextPoolWorkStealing.AvoidForCurrentThread;
                    return ctx;
                });
        }

        private void CleanThreadLocalState()
        {
            StackNode<T> current;
            try
            {
                current = MaybeGetCurrentContextStack()?.Head;

                if (current == null)
                    return;

                _contextStacksByThreadId.TryRemove(NativeMemory.CurrentThreadStats.Id, out _);
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
            var copy = _threadIds;
            for (int i = 0; i < copy.Length; i++)
            {
                var id = copy[i].ThreadId;
                if (id == -1)
                    continue;

                if (_contextStacksByThreadId.TryGetValue(id, out var ctx))
                    yield return ctx;
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
                threadHeader = GetCurrentContextStack();
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
                foreach (var kvp in EnumerateAllThreadContexts())
                {
                    kvp.Dispose();
                }
                _contextStacksByThreadId.Clear();
                _threadIds = Array.Empty<ThreadIdHolder>();
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

    public static class JsonContextPoolWorkStealing
    {
        [ThreadStatic]
        public static bool AvoidForCurrentThread;
    }

}
