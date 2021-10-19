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
        public class DebugProxy
        {
            private readonly ConcurrentSet<T> _parent;

            public DebugProxy(ConcurrentSet<T> parent)
            {
                _parent = parent;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public object[] Items => _parent.Cast<object>().ToArray();
        }

        private readonly ConcurrentDictionary<T, object> _inner;

        public ConcurrentSet()
        {
            _inner = new ConcurrentDictionary<T, object>();
        }

        public ConcurrentSet(IEqualityComparer<T> comparer)
        {
            _inner = new ConcurrentDictionary<T, object>(comparer);
        }

        public int Count => _inner.Count;

        public void Add(T item)
        {
            TryAdd(item);
        }

        public bool TryAdd(T item)
        {
            return _inner.TryAdd(item, null);
        }

        public bool Contains(T item)
        {
            return _inner.ContainsKey(item);
        }

        public bool TryRemove(T item)
        {
            object _;
            return _inner.TryRemove(item, out _);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _inner.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            _inner.Clear();
        }

        public override string ToString()
        {
            return Count.ToString("#,#", CultureInfo.InvariantCulture);
        }
    }
}
