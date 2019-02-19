using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Sparrow.Server.Collections
{
    public class AsyncQueue<T> 
    {
        private readonly ConcurrentQueue<T> _inner = new ConcurrentQueue<T>();
        private readonly AsyncManualResetEvent _event = new AsyncManualResetEvent();
        public int Count => _inner.Count;

        public void Enqueue(T item)
        {
            _inner.Enqueue(item);
            _event.Set();
        }

        public async Task<T> DequeueAsync()
        {
            T result;
            while (_inner.TryDequeue(out result) == false)
            {
                await _event.WaitAsync().ConfigureAwait(false);
                _event.Reset();
            }
            return result;
        }

        public async Task<Tuple<bool, T>> TryDequeueAsync(TimeSpan timeout)
        {
            T result;
            while (_inner.TryDequeue(out result) == false)
            {
                if (await _event.WaitAsync(timeout).ConfigureAwait(false) == false)
                    return Tuple.Create(false, default(T));
                _event.Reset();
            }
            return Tuple.Create(true, result);
        }

        public async Task<Tuple<bool, TValue>> TryDequeueOfTypeAsync<TValue>(TimeSpan timeout) where TValue : T
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                T result;
                while (_inner.TryDequeue(out result) == false)
                {
                    var wait = timeout - sp.Elapsed;

                    if (wait < TimeSpan.Zero)
                        wait = TimeSpan.Zero;

                    if (await _event.WaitAsync(wait).ConfigureAwait(false) == false)
                        return Tuple.Create(false, default(TValue));
                    _event.Reset();
                }

                if (result is TValue)
                    return Tuple.Create(true, (TValue)result);
            }
        }
    }
}
