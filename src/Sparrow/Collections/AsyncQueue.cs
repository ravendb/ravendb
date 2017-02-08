using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Collections
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
                await _event.WaitAsync();
                _event.Reset();
            }
            return result;
        }

        public async Task<Tuple<bool, T>> TryDequeueAsync(TimeSpan timeout)
        {
            T result;
            while (_inner.TryDequeue(out result) == false)
            {
                if (await _event.WaitAsync(timeout) == false)
                    return Tuple.Create(false, default(T));
                _event.Reset();
            }
            return Tuple.Create(true, result);
        }
        
    }
}