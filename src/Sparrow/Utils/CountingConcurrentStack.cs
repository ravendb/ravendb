using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Sparrow.Utils
{
    internal sealed class CountingConcurrentStack<TItem>
    {
        private readonly ConcurrentStack<TItem> _stack = new ConcurrentStack<TItem>();

        private long _count;

        public bool IsEmpty => _stack.IsEmpty;

        public long Count => Interlocked.Read(ref _count);

        public bool TryPop(out TItem item)
        {
            if (_stack.TryPop(out item) == false)
            {
                item = default;
                return false;
            }

            Interlocked.Decrement(ref _count);
            return true;
        }

        public void Push(TItem item)
        {
            _stack.Push(item);
            Interlocked.Increment(ref _count);
        }

        public IEnumerator<TItem> GetEnumerator()
        {
            return _stack.GetEnumerator();
        }
    }
}
