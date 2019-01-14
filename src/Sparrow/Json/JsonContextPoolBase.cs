using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public class ThreadIdHolder
    {
        public int ThreadId;
        public override string ToString()
        {
            return ThreadId.ToString();
        }
    }

#if DEBUG
    public interface ThreadIDsHolderInterface
    {
        ThreadIdHolder[] ThreadIDs { get; }
    }

    public static class JsonContextPoolsHolder
    {
        public static ConcurrentSet<ThreadIDsHolderInterface> AllContexts = new ConcurrentSet<ThreadIDsHolderInterface>();
    } 
#endif

    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
#if DEBUG
        , ThreadIDsHolderInterface
#endif
        where T : JsonOperationContext
    {
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private ConcurrentDictionary<int, ContextStack> _contextStacksByThreadId = new ConcurrentDictionary<int, ContextStack>();
        private ThreadIdHolder[] _threadIds = Array.Empty< ThreadIdHolder>();

        private static ConcurrentBag<ThreadIdHolder[]> AllThreadIdHolders = new ConcurrentBag<ThreadIdHolder[]>();

        public ThreadIdHolder[] ThreadIDs => _threadIds;

        private readonly NativeMemoryCleaner<ContextStack, T> _nativeMemoryCleaner;
        private bool _disposed;
        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        // because this is a finalizer object, we want to pool them to avoid having too many items in the finalization queue
        private static ObjectPool<ContextStack> _contextStackPool = new ObjectPool<ContextStack>(() => new ContextStack());

        private class ContextStackThreadReleaser
        {
            private HashSet<JsonContextPoolBase<T>> _parents = new HashSet<JsonContextPoolBase<T>>();
            int _threadId;
            public List<ThreadIdHolder> ThreadIdHolders = new List<ThreadIdHolder>();            

            public ContextStackThreadReleaser()
            {
                _threadId = NativeMemory.CurrentThreadStats.Id;
            }

            ~ContextStackThreadReleaser()
            {
                foreach (var item in ThreadIdHolders)
                {
                    Debug.Assert(item.ThreadId == _threadId);
                    item.ThreadId = -1;
                }
                // we remove the references from the thread dictionary
                // it is possible that the pool is no longer referenced and was collected, and as such
                // the finalizers for the context stack values were already run, if not, they will run soon
                // anyway, so we just clear it.
                foreach (var parent in _parents)
                {
                    parent._contextStacksByThreadId.TryRemove(_threadId, out var contextStack);
                }
            }

            public bool AddContextPool(JsonContextPoolBase<T> parent)
            {
                return _parents.Add(parent);
            }

            public void RemoveContextPool(JsonContextPoolBase<T> parent)
            {
                _parents.Remove(parent);
            }

            public void RemoveThreadIdHolder(ThreadIdHolder holder)
            {
                ThreadIdHolders.Remove(holder);
            }
        }

        [ThreadStatic]
        private static ContextStackThreadReleaser _releaser;
        

        private void EnsureCurrentThreadContextWillBeReleased(int currentThreadId)
        {
            if (_releaser == null)
            {
                _releaser = new ContextStackThreadReleaser();
            }

            if (_releaser.AddContextPool(this) == false)
                return;

#if DEBUG
            AssertThreadIDsUniqueness(); 
#endif

            while (true)
            {
                var copy = _threadIds;
                for (int i = 0; i < copy.Length; i++)
                {
                    if (copy[i].ThreadId == -1)
                    {
                        if(Interlocked.CompareExchange(ref copy[i].ThreadId, currentThreadId, -1) == -1)
                        {
                            _releaser.ThreadIdHolders.Add(copy[i]);
                            return;
                        }
                        return;
                    }
                }
                
                var threads = new ThreadIdHolder[copy.Length + 1];
                Array.Copy(copy, threads, copy.Length);
                threads[copy.Length] = new ThreadIdHolder
                {
                    ThreadId = currentThreadId
                };

                if (Interlocked.CompareExchange(ref _threadIds, threads, copy) == copy)
                {
                    _releaser.ThreadIdHolders.Add(threads[copy.Length]);
                    break;
                }
            }
        }

#if DEBUG
        private void AssertThreadIDsUniqueness()
        {
            var ids = new HashSet<int>();

            var copy = _threadIds;

            foreach (var item in copy.Where(x => x.ThreadId != -1))
            {
                if (ids.Add(item.ThreadId) == false)
                {                    
                    throw new InvalidOperationException($"Double thread id spotted in {nameof(JsonContextPoolBase<T>)}");
                }
            }

            foreach (var context in JsonContextPoolsHolder.AllContexts)
            {
                copy = context.ThreadIDs;
                ids.Clear();
                foreach (var item in copy.Where(x => x.ThreadId != -1))
                {
                    if (ids.Add(item.ThreadId) == false)
                    {
                        throw new InvalidOperationException($"Double thread id spotted in {nameof(JsonContextPoolBase<T>)} of a different context");
                    }
                }
            }
        } 
#endif

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

#if DEBUG
            JsonContextPoolsHolder.AllContexts.Add(this); 
#endif
        }

        private ContextStack MaybeGetCurrentContextStack()
        {
            _contextStacksByThreadId.TryGetValue(NativeMemory.CurrentThreadStats.Id, out var x);
            return x;
        }

        private ContextStack GetCurrentContextStack()
        {
            return _contextStacksByThreadId.GetOrAdd(NativeMemory.CurrentThreadStats.Id, 
                currentThreadId =>
                {
                    EnsureCurrentThreadContextWillBeReleased(currentThreadId);
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

                _contextStacksByThreadId.TryRemove(NativeMemory.CurrentThreadStats.Id, out _);

                // we want to clear our JsonContextPool's current thread's state from the _releaser, but to avoid touching any other states
                foreach (var threadIdHolder in _threadIds)
                {
                    if (Interlocked.CompareExchange(ref threadIdHolder.ThreadId, -1, NativeMemory.CurrentThreadStats.Id) == NativeMemory.CurrentThreadStats.Id)
                    {
                        _releaser.RemoveThreadIdHolder(threadIdHolder);
                        break;
                    }
                }

                _releaser.RemoveContextPool(this);

                if (current == null)
                    return;

                
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

#if DEBUG
                JsonContextPoolsHolder.AllContexts.TryRemove(this); 
#endif
                _cts.Cancel();
                _disposed = true;
                ThreadLocalCleanup.ReleaseThreadLocalState -= CleanThreadLocalState;
                _nativeMemoryCleaner.Dispose();

#if Debug
                var allThreadsContextStacks = new HashSet<ContextStack>();
#endif
                foreach (var kvp in EnumerateAllThreadContexts())
                {
#if Debug
                    if (allThreadsContextStacks.Add(kvp) == false)
                    {
                        throw new InvalidOperationException("threads list is not unique");
                    }
#endif

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
