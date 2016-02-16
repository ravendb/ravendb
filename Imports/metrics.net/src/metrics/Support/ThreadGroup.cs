using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace metrics.Support
{
    /// <summary>
    /// Provides a thread group for tracking
    /// </summary>
    internal class ThreadGroup : IEnumerable<Thread>
    {
        private readonly string _name;
        private readonly ConcurrentDictionary<int,Thread> _threads;

        protected internal ThreadGroup(string name)
        {
            _name = name;
            _threads = new ConcurrentDictionary<int, Thread>();
        }
        
        public void Add(Thread thread)
        {
            if (!_threads.ContainsKey(thread.ManagedThreadId))
            {
                _threads.AddOrUpdate(thread.ManagedThreadId, thread, (i, t) => t);
            }
        }

        public bool Remove(Thread thread)
        {
            Thread removed;
            return _threads.TryRemove(thread.ManagedThreadId, out removed);
        }

        public string Name
        {
            get { return _name; }
        }

        public IEnumerator<Thread> GetEnumerator()
        {
            return _threads.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}