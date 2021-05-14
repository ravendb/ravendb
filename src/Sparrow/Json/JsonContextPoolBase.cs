using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        private readonly object _locker = new object();
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<JsonContextPoolBase<T>>("Memory");

        private bool _disposed;

        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private long _generation;

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly long _maxContextSizeToKeepInBytes;
        private readonly long _maxNumberOfContextsToKeepInGlobalStack;
        private long _numberOfContextsDisposedInGlobalStack;

        private readonly PerCoreContainer<T> _perCoreCache = new PerCoreContainer<T>();
        private readonly CountingConcurrentStack<T> _globalStack = new CountingConcurrentStack<T>();
        private readonly Timer _cleanupTimer;

        protected JsonContextPoolBase()
        {
            _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
            _maxContextSizeToKeepInBytes = long.MaxValue;
            _maxNumberOfContextsToKeepInGlobalStack = PlatformDetails.Is32Bits == false
                ? 4096
                : 1024;
        }

        protected JsonContextPoolBase(Size? maxContextSizeToKeep)
            : this()
        {
            if (maxContextSizeToKeep.HasValue)
                _maxContextSizeToKeepInBytes = maxContextSizeToKeep.Value.GetValue(SizeUnit.Bytes);
        }

        protected JsonContextPoolBase(Size? maxContextSizeToKeep, long? maxNumberOfContextsToKeepInGlobalStack)
            : this(maxContextSizeToKeep)
        {
            if (maxNumberOfContextsToKeepInGlobalStack.HasValue)
                _maxNumberOfContextsToKeepInGlobalStack = maxNumberOfContextsToKeepInGlobalStack.Value;
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

            // currently we have nothing to do here
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();

            while (_perCoreCache.TryPull(out context))
            {
                if (context.InUse.Raise() == false)
                    continue;
                // This what ensures that we work correctly with races from other threads
                // if there is a context switch at the wrong time
                context.Renew();
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            if (TryGetFromStack(_globalStack, out context))
            {
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            // no choice, got to create it
            context = CreateContext();
            context.PoolGeneration = _generation;
            return new ReturnRequestContext
            {
                Parent = this,
                Context = context
            };

            static bool TryGetFromStack(CountingConcurrentStack<T> stack, out T context)
            {
                context = default;

                if (stack == null || stack.IsEmpty)
                    return false;

                while (stack.TryPop(out context))
                {
                    if (context.InUse.Raise() == false)
                        continue;

                    context.Renew();
                    return true;
                }

                return false;
            }
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                var parent = Parent;
                if (parent == null)
                    return; // disposed already

                Parent = null;
                 
                if (Context.DoNotReuse)
                {
                    Context.Dispose();
                    return;
                }

                if (Context.AllocatedMemory > parent._maxContextSizeToKeepInBytes)
                {
                    Context.Dispose();
                    return;
                }

                if (parent.LowMemoryFlag.IsRaised() && Context.PoolGeneration < parent._generation)
                {
                    // releasing all the contexts which were created before we got the low memory event
                    Context.Dispose();
                    return;
                }

                Context.Reset();
                // These contexts are reused, so we don't want to use LowerOrDie here.
                Context.InUse.Lower();
                Context.InPoolSince = DateTime.UtcNow;

                parent.Push(Context);

                Context = null;
            }
        }

        private DateTime _lastPerCoreCleanup = DateTime.UtcNow;
        private readonly TimeSpan _perCoreCleanupInterval = TimeSpan.FromMinutes(5);

        private DateTime _lastGlobalStackRebuild = DateTime.UtcNow;
        private readonly TimeSpan _globalStackRebuildInterval = TimeSpan.FromMinutes(15);

        private void Cleanup(object _)
        {
            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                var currentTime = DateTime.UtcNow;
                var idleTime = TimeSpan.FromMinutes(5);
                var currentGlobalStack = _globalStack;

                var perCoreCleanupNeeded = currentGlobalStack.IsEmpty || currentTime - _lastPerCoreCleanup >= _perCoreCleanupInterval;
                if (perCoreCleanupNeeded)
                {
                    _lastPerCoreCleanup = currentTime;

                    foreach (var current in _perCoreCache)
                    {
                        var context = current.Item;
                        var timeInPool = currentTime - context.InPoolSince;
                        if (timeInPool <= idleTime)
                            continue;

                        if (context.InUse.Raise() == false)
                            continue;

                        _perCoreCache.Remove(current.Item, current.Pos);
                        context.Dispose();
                    }

                    return;
                }

                using (var globalStackEnumerator = currentGlobalStack.GetEnumerator())
                {
                    while (globalStackEnumerator.MoveNext())
                    {
                        var context = globalStackEnumerator.Current;

                        var timeInPool = currentTime - context.InPoolSince;
                        if (timeInPool <= idleTime)
                            continue;

                        if (context.InUse.Raise() == false)
                            continue;

                        context.Dispose();
                        _numberOfContextsDisposedInGlobalStack++;
                    }
                }

                var globalStackRebuildNeeded = currentTime - _lastGlobalStackRebuild >= _globalStackRebuildInterval;

                if (globalStackRebuildNeeded && _numberOfContextsDisposedInGlobalStack > 0)
                {
                    _lastGlobalStackRebuild = currentTime;

                    _numberOfContextsDisposedInGlobalStack = 0;

                    var localStack = new CountingConcurrentStack<T>();

                    while (currentGlobalStack.TryPop(out var context))
                    {
                        if (context.InUse.Raise() == false)
                            continue;

                        context.InUse.Lower();
                        localStack.Push(context);
                    }

                    while (localStack.TryPop(out var context))
                        currentGlobalStack.Push(context);
                }
            }
            catch (Exception e)
            {
                Debug.Assert(e is OutOfMemoryException, $"Expecting OutOfMemoryException but got: {e}");
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Error during cleanup.", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        private void Push(T context)
        {
            if (_perCoreCache.TryPush(context))
                return;

            if (LowMemoryFlag.IsRaised())
            {
                context.Dispose();
                return;
            }

            var currentGlobalStack = _globalStack;

            // couldn't find a place for it, let's add it to the global list
            if (currentGlobalStack.Count >= _maxNumberOfContextsToKeepInGlobalStack)
            {
                context.Dispose();
                return;
            }

            currentGlobalStack.Push(context);
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;

            lock (_locker)
            {
                if (_disposed)
                    return;

                _cts.Cancel();
                _disposed = true;
                _cleanupTimer.Dispose();

                ClearStack(_globalStack);

                foreach (var context in _perCoreCache.EnumerateAndClear())
                {
                    context.Dispose();
                }
            }

            static void ClearStack(CountingConcurrentStack<T> stack)
            {
                if (stack == null || stack.IsEmpty)
                    return;

                while (stack.TryPop(out var context))
                {
                    context.Dispose();
                }
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            if (LowMemoryFlag.Raise())
            {
                Interlocked.Increment(ref _generation);
            }

            if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
                return;

            if (_isExtremelyLowMemory.Raise() == false)
                return;

            ClearStack(_globalStack);

            foreach (var context in _perCoreCache.EnumerateAndClear())
            {
                if (context.InUse.Raise())
                    context.Dispose();
            }

            static void ClearStack(CountingConcurrentStack<T> stack)
            {
                if (stack == null || stack.IsEmpty)
                    return;

                while (stack.TryPop(out var context))
                {
                    if (context.InUse.Raise())
                        context.Dispose();
                }
            }
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
            _isExtremelyLowMemory.Lower();
        }
    }
}
