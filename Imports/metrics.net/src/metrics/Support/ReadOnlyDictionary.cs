using System.Collections;
using System.Collections.Generic;
using metrics.Core;

namespace metrics.Support
{
    /// <summary>
    /// Provides an immutable dictionary
    /// </summary>
    internal class ReadOnlyDictionary<T, TK> : IDictionary<T, TK> where TK : ICopyable<TK>
    {
        private readonly IDictionary<T, TK> _inner;

        public ReadOnlyDictionary(ICollection<KeyValuePair<T, TK>> inner)
        {
            _inner = new Dictionary<T, TK>(inner.Count);
            foreach(var entry in inner)
            {
                _inner.Add(entry.Key, entry.Value.Copy);
            }
        }

        public IEnumerator<KeyValuePair<T, TK>> GetEnumerator()
        {
            return _inner.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<T, TK> item) { /* read-only */ }

        public void Clear() { /* read-only */ }

        public bool Contains(KeyValuePair<T, TK> item)
        {
            return _inner.Contains(item);
        }

        public void CopyTo(KeyValuePair<T, TK>[] array, int arrayIndex)
        {
            CopyInner().CopyTo(array, arrayIndex);
        }

        private IDictionary<T, TK> CopyInner()
        {
            IDictionary<T, TK> copy = new Dictionary<T, TK>(_inner.Count);
            foreach (var entry in _inner)
            {
                copy.Add(entry.Key, entry.Value.Copy);
            }
            return copy;
        }

        public bool Remove(KeyValuePair<T, TK> item) { return false; /* read-only */ }

        public int Count
        {
            get { return _inner.Count; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        public bool ContainsKey(T key)
        {
            return _inner.ContainsKey(key);
        }

        public void Add(T key, TK value) { /* read-only */ }

        public bool Remove(T key) { return false; /* read-only */ }

        public bool TryGetValue(T key, out TK value)
        {
            var result = _inner.TryGetValue(key, out value);
            value = value.Copy;
            return result;
        }

        public TK this[T key]
        {
            get
            {
                return _inner[key].Copy;
            }
            set { /* read-only */ }
        }

        public ICollection<T> Keys
        {
            get { return CopyInner().Keys; }
        }

        public ICollection<TK> Values
        {
            get { return CopyInner().Values; }
        }
    }
}
