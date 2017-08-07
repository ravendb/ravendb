using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        private readonly ObjectPool<T, JsonOperationContextResetBehavior, ThreadAwareBehavior> _contextPool;
        
        private struct JsonOperationContextResetBehavior : IResetSupport<JsonOperationContext>
        {
            public void Reset(JsonOperationContext value)
            {
                value.Reset();
                value.InPoolSince = DateTime.UtcNow;
            }
        }

        private readonly long _idleTime;
        private readonly Timer _timer;
        
        private struct EvictionPolicy : IEvictionStrategy<T>
        {
            private readonly long _now;
            private readonly long _idle;

            public EvictionPolicy(long now, long idle)
            {
                this._now = now;
                this._idle = idle;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CanEvict(T item)
            {
                var timeInPool = _now - item.InPoolSince.Ticks;
                return timeInPool > _idle;
            }
        }
               
        private bool _disposed;
        
        protected LowMemoryFlag LowMemoryFlag = new LowMemoryFlag();
        
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();


        protected JsonContextPoolBase(int poolSize = 2048, int bucketSize = 16)
        {
            _contextPool = new ObjectPool<T, JsonOperationContextResetBehavior, ThreadAwareBehavior>(CreateContext, poolSize, new ThreadAwareBehavior(bucketSize));

            var period = TimeSpan.FromMinutes(5);
            _idleTime = TimeSpan.FromMinutes(1).Ticks;
            _timer = new Timer(CleanOldest, null, period, period);

            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        protected void CleanOldest(object state)
        {
            var evictionPolicy = new EvictionPolicy(DateTime.UtcNow.Ticks, _idleTime);
            _contextPool.Clear(evictionPolicy);
        }

        public void Clean()
        {
            // we are expecting to be called here when there is no
            // more work to be done, and we want to release resources
            // to the system

            _contextPool.Clear(false);
        }


        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var result = _contextPool.AllocateInContext();
            context = result.Value;
            context.Renew();

            return result;
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            var result = _contextPool.AllocateInContext();
            context = result.Value;
            context.Renew();
            
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
                _timer.Dispose();
                _disposed = true;
            }
        }

        public void LowMemory()
        {
            if (Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 1, 0) != 0)
                return;
            
            _contextPool.Clear();
        }

        public void LowMemoryOver()
        {
            Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 0, 1);
        }
    }
}