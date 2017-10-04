using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace Sparrow.Collections
{
    [DebuggerTypeProxy(typeof(ConcurrentSet<>.DebugProxy))]
    public class ConcurrentSet<T> : IEnumerable<T>
    {
        // accessing the ConcurrentDictionary.Count will cause it to aquire a 
        // lock on the entire object, but we want it to be fast, because we use 
        // it to check for existence. 
        private int _count;

        public class DebugProxy
        {
            private ConcurrentSet<T> parent;

            public DebugProxy(ConcurrentSet<T> parent)
            {
                this.parent = parent;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public object[] Items
            {
                get { return parent.Cast<object>().ToArray(); }
            }
        }

        private readonly ConcurrentDictionary<T, object> inner;

        public ConcurrentSet()
        {
            inner = new ConcurrentDictionary<T, object>();
        }

        public ConcurrentSet(IEqualityComparer<T> comparer)
        {
            inner = new ConcurrentDictionary<T, object>(comparer);
        }

        public int Count
        {
            get { return _count; }
        }

        public void Add(T item)
        {
            TryAdd(item);
        }

        public bool TryAdd(T item)
        {
            var result = inner.TryAdd(item, null);
            if (result)
                Interlocked.Increment(ref _count);
            return result;
        }

        public bool Contains(T item)
        {
            return inner.ContainsKey(item);
        }

        public bool TryRemove(T item)
        {
            object _;
            var result = inner.TryRemove(item, out _);
            if (result)
                Interlocked.Decrement(ref _count);
            return result;
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var item in inner)
            {
                yield return item.Key;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void RemoveWhere(Func<T, bool> predicate)
        {
            foreach (var item in inner.Where(item => predicate(item.Key)))
            {
                object value;
                if (inner.TryRemove(item.Key, out value))
                    Interlocked.Decrement(ref _count);
            }
        }

        public void Clear()
        {
            var currentCount = Volatile.Read(ref _count);
            inner.Clear();
            Interlocked.Add(ref _count, -currentCount);
        }

        public override string ToString()
        {
            return Count.ToString("#,#", CultureInfo.InvariantCulture);
        }
    }

}
