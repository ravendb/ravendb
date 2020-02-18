using System.Collections.Concurrent;
using System.Threading;
using System.Transactions;

namespace Sparrow.Collections
{
    public class LimitedConcurrentSet<T>
    {
        private readonly int _max;
        private int _count;
        private ConcurrentQueue<T> _q = new ConcurrentQueue<T>();

        public LimitedConcurrentSet(int max)
        {
            _max = max;
        }

        public void Clear()
        {
            while (_q.TryDequeue(out _))
            {
                Interlocked.Decrement(ref _count);
            }
        }

        public bool Enqueue(T item, int timeout)
        {
            var count = Interlocked.Increment(ref _count);
            if (count >= _max)
            {
                if (ContendedCodePath(timeout) == false) 
                    return false;
            }
            _q.Enqueue(item);
            return true;
        }

        public bool TryDequeue(out T item)
        {
            bool result = _q.TryDequeue(out item);
            if (result)
            {
                Interlocked.Decrement(ref _count);
            }
            return result;
        }

        private bool ContendedCodePath(int timeout)
        {
            var timeToWait = 2;

            while (true)
            {
                Interlocked.Decrement(ref _count);
                if (timeToWait > timeout)
                    timeToWait = timeout;

                timeout -= timeToWait;
                if (timeout <= 0)
                    return false;
                
                Thread.Sleep(timeToWait);
                var count = Interlocked.Increment(ref _count);
                if (count < _max)
                    return true;

                timeToWait += 16;
            }
        }
    }
}
