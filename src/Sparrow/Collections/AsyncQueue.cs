using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Collections
{
    public class AsyncQueue<T> : IDisposable
    {
        private readonly ConcurrentQueue<T> _inner = new ConcurrentQueue<T>();
        private readonly AsyncManualResetEvent _event = new AsyncManualResetEvent();
        private bool _disposed;
        public int Count => _inner.Count;

        public void Enqueue(T item)
        {
            EnsureNotDisposed();

            _inner.Enqueue(item);
            _event.Set();
        }

        public async Task<T> DequeueAsync()
        {
            EnsureNotDisposed();

            T result;
            while (_inner.TryDequeue(out result) == false)
            {
                EnsureNotDisposed();
                await _event.WaitAsync();
                _event.Reset();
            }
            return result;
        }

        public async Task<Tuple<bool, T>> TryDequeueAsync(TimeSpan timeout)
        {
            EnsureNotDisposed();

            T result;
            while (_inner.TryDequeue(out result) == false)
            {
                EnsureNotDisposed();
                if (await _event.WaitAsync(timeout) == false)
                    return Tuple.Create(false, default(T));
                _event.Reset();
            }
            return Tuple.Create(true, result);
        }

        public void Dispose()
        {
            _disposed = true;
            _event.Set();
        }

        private void EnsureNotDisposed()
        {
            if (_disposed)
                throw new OperationCanceledException("The async queue was disposed and cannot be used anymore");
        }
    }
}