using System;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        private struct JsonOperationContextResetBehavior : IResetSupport<JsonOperationContext>
        {
            public void Reset(JsonOperationContext value)
            {
                value.Reset();
                value.InPoolSince = DateTime.UtcNow;
            }
        }
        
        private ObjectPool<T, JsonOperationContextResetBehavior, ThreadAwareBehavior> _contextPool;
        
        private bool _disposed;
        protected LowMemoryFlag LowMemoryFlag = new LowMemoryFlag();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private const int PoolSize = 2048;

        protected JsonContextPoolBase()
        {
            _contextPool = new ObjectPool<T, JsonOperationContextResetBehavior, ThreadAwareBehavior>(CreateContext, PoolSize);

            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            T ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;
            context.Renew();

            return disposable;
        }

        public void Clean()
        {
            // we are expecting to be called here when there is no
            // more work to be done, and we want to release resources
            // to the system

            _contextPool = new ObjectPool<T, JsonOperationContextResetBehavior, ThreadAwareBehavior>(CreateContext, PoolSize);
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var result = _contextPool.AllocateInContext();
            context = result.Value;
            return result;          
        }        

        protected abstract T CreateContext();    

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
            }
        }

        public void LowMemory()
        {
            if (Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 1, 0) != 0)
                return;
        }

        public void LowMemoryOver()
        {
            Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 0, 1);
        }
    }
}