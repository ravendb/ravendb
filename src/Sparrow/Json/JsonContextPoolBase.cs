using System;
using System.Threading;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IMemoryContextPool
        where T : JsonOperationContext
    {
        private readonly IRavenLogger _logger;
        private readonly object _locker = new object();

        private bool _disposed;

        protected SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
        private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();
        private long _generation;

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly long _maxContextSizeToKeepInBytes;

        private readonly PerCoreContainer<T> _contextBuffer = new PerCoreContainer<T>(PlatformDetails.Is32Bits ? 4 * 1024 : 64 * 1024);

        protected JsonContextPoolBase(IRavenLogger logger, int? perCoreSlots = null)
        {
            _logger = logger;
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
            _maxContextSizeToKeepInBytes = long.MaxValue;

            var slots = perCoreSlots ?? (PlatformDetails.Is32Bits ? 8 : 16);
        }

        protected JsonContextPoolBase(Size? maxContextSizeToKeep, IRavenLogger logger, int? perCoreSlots = null)
            : this(logger, perCoreSlots)
        {
            if (maxContextSizeToKeep.HasValue)
                _maxContextSizeToKeepInBytes = maxContextSizeToKeep.Value.GetValue(SizeUnit.Bytes);
        }


        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            var disposable = AllocateOperationContext(out T ctx);
            context = ctx;

            return disposable;
        }

        public void Clean()
        {
            // intentionally no-op by default
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();

            while (_contextBuffer.TryPull(out context))
            {
                // We must raise InUse flag before doing anything else with the context
                // to prevent it from being disposed by another thread
                if (context.InUse.Raise() == false)
                {
                    // Context is disposed or already in use. Due to lock-free ring buffer semantics,
                    // once we pull it from the buffer, no other thread can get it, so this means
                    // the context was disposed. Do NOT put it back in the pool - just skip it.
                    continue;
                }

                try
                {
                    context.Renew();
                    return new ReturnRequestContext
                    {
                        Parent = this,
                        Context = context
                    };
                }
                catch
                {
                    // If Renew() fails (e.g., context was disposed), lower the flag and continue
                    context.InUse.Lower();
                    continue;
                }
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

        private sealed class ReturnRequestContext : IDisposable
        {
            public T Context;
            public JsonContextPoolBase<T> Parent;

            public void Dispose()
            {
                var parent = Interlocked.Exchange(ref Parent, null);             
                if (parent == null)
                    return; // disposed already

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

                parent.Push(Context);

                Context = null;
            }
        }

        private void Push(T context)
        {
            // If we are in low memory mode, dispose the context immediately
            if (LowMemoryFlag.IsRaised())
            {
                context.Dispose();
                return;
            }

            // These contexts are reused, so we don't want to use LowerOrDie here.
            context.Reset();
            context.InUse.Lower();
            context.InPoolSince = DateTime.UtcNow;

            // Try to enqueue the context back into the ring buffer
            if (_contextBuffer.TryPush(context) == false)
            {
                // Ring buffer is full, dispose the context
                context.Dispose();
            }
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

            _contextBuffer.Cleanup(new RemoveAllAndDisposePolicy<T>());
        }

        public void LowMemoryOver()
        {
            LowMemoryFlag.Lower();
            _isExtremelyLowMemory.Lower();
        }
    }
}
