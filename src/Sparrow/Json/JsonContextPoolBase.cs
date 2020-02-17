using System;
using System.Collections.Concurrent;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable 
        where T : JsonOperationContext
    {
        private long _generation;
        private bool _disposed;

        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly long _maxContextSizeToKeepInBytes;

        private readonly T[][] _perCoreContexts;
        private readonly ConcurrentQueue<T> _globalQueue = new ConcurrentQueue<T>();
        private readonly Timer _idleTimer;

        protected JsonContextPoolBase()
        {
            _perCoreContexts = new T[Environment.ProcessorCount][];
            for (int i = 0; i < _perCoreContexts.Length; i++)
            {
                _perCoreContexts[i] = new T[2];
            }
            _idleTimer = new Timer(IdleTimer, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);

        }

        protected JsonContextPoolBase(Size? maxContextSizeToKeepInMb) : this()
        {
            _maxContextSizeToKeepInBytes = maxContextSizeToKeepInMb?.GetValue(SizeUnit.Bytes) ?? long.MaxValue;
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
            var coreItems = _perCoreContexts[CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreContexts.Length];

            for (int i = 0; i < coreItems.Length; i++)
            {
                context = coreItems[i];
                if (context == null)
                    continue;

                if (Interlocked.CompareExchange(ref coreItems[i], null, context) != context)
                    continue;

                if (context.InUse.Raise() == false)
                {
                    // This what ensures that we work correctly with races from other threads
                    // if there is a context switch at the wrong time
                    continue;
                }
                context.Renew();
                return new ReturnRequestContext
                {
                    Parent = this,
                    Context = context
                };
            }

            while (_globalQueue.TryDequeue(out context))
            {
                if (context.InUse.Raise() == false)
                    continue;

                context.Renew();
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
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                if (Parent == null)
                    return; // disposed already

                if (Context.DoNotReuse)
                {
                    Context.Dispose();
                    return;
                }

                if (Context.AllocatedMemory > Parent._maxContextSizeToKeepInBytes)
                {
                    Context.Dispose();
                    return;
                }

                if (Parent.LowMemoryFlag.IsRaised() && Context.PoolGeneration < Parent._generation)
                {
                    // releasing all the contexts which were created before we got the low memory event
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

        private void IdleTimer(object _)
        {
            var currentTime = DateTime.UtcNow;
            TimeSpan idleTime = TimeSpan.FromMinutes(5);

            if (_globalQueue.IsEmpty)
            {
                // we have nothing in global, let's check if we can clear the per core
                foreach (var current in _perCoreContexts)
                {
                    for (var index = 0; index < current.Length; index++)
                    {
                        var context = current[index];
                        if (currentTime - context.InPoolSince > idleTime)
                            continue;

                        if (!context.InUse.Raise())
                            continue;

                        Interlocked.CompareExchange(ref current[index], null, context);
                        context.Dispose();
                    }
                }

                return;
            }

            while (_globalQueue.TryPeek(out var context))
            {
                if (currentTime - context.InPoolSince < idleTime)
                {
                    break;
                }

                if (_globalQueue.TryDequeue(out context) == false)
                    break;

                if (currentTime - context.InPoolSince > idleTime)
                {
                    if (context.InUse.Raise())
                        context.Dispose();
                }
                else
                {
                    // was checked out, meaning there is activity, we are done now
                    _globalQueue.Enqueue(context);
                    break;
                }
            }
        }

        private void Push(T context)
        {
            int currentProcessorId = CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreContexts.Length;
            var core = _perCoreContexts[currentProcessorId];

            for (int i = 0; i < core.Length; i++)
            {
                if (core[i] != null)
                    continue;
                if (Interlocked.CompareExchange(ref core[i], context, null) == null)
                    return;
            }

            if (core.Length < 64)
            {
                var newCore = new T[core.Length * 2];
                Array.Copy(core, 0, newCore, 0, core.Length);
                newCore[core.Length] = context;
                if (Interlocked.CompareExchange(ref _perCoreContexts[currentProcessorId], newCore, core) == core)
                    return;
            }

            if (LowMemoryFlag.IsRaised())
            {
                context.Dispose();
                return;
            }

            // couldn't find a place for it, let's add it to the global list
            _globalQueue.Enqueue(context);
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
                _idleTimer.Dispose();

                while (_globalQueue.TryDequeue(out var result))
                {
                    result.Dispose();
                }

                foreach (var coreContext in _perCoreContexts)
                {
                    for (int i = 0; i < coreContext.Length; i++)
                    {
                        coreContext[i]?.Dispose();
                        coreContext[i] = null;
                    }

                }
            }
        }

        public void LowMemory()
        {
            if (!LowMemoryFlag.Raise())
                return;

            Interlocked.Increment(ref _generation);

            while (_globalQueue.TryDequeue(out var result))
            {
                if (result.InUse.Raise())
                    result.Dispose();
            }

            foreach (var coreContext in _perCoreContexts)
            {
                for (int i = 0; i < coreContext.Length; i++)
                {
                    var context = coreContext[i];
                    if (context != null && context.InUse.Raise())
                    {
                        context.Dispose();
                        Interlocked.CompareExchange(ref coreContext[i], null, context);
                    }
                }
            }
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
        }
    }
}
