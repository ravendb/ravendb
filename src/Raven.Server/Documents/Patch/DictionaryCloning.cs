using System;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Linq;
using V8.Net;

namespace Raven.Server.Documents.Patch
{
    public class DictionaryCloningValue<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable
        where TValue : IDisposable, IClonable<TValue>
    {
        private Dictionary<TKey, TValue> _dict;

        public DictionaryCloningValue()
        {
            _dict = new Dictionary<TKey, TValue>();
        }

        public void Dispose()
        {  
            GC.SuppressFinalize(this);
            
            Clear();
            _dict = null;
        }

        public void Clear()
        {
            foreach (var kvp in _dict)
            {
                kvp.Value.Dispose();
            }

            _dict.Clear();
        }


        public TValue this[TKey key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _dict[key];

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                var valueNew = value.Clone();
                try
                {
                    _dict[key] = valueNew;
                }
                catch 
                {
                    valueNew.Dispose();
                    throw;
                }
            }
        }

        public ICollection<TKey> Keys
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _dict.Keys;
        }

        public ICollection<TValue> Values
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _dict.Values;
        }

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _dict.Count;
        }

        public bool IsReadOnly
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((ICollection<KeyValuePair<TKey, TValue>>)_dict).IsReadOnly;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(TKey key)
        {
            return _dict.ContainsKey(key);
        }

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dict.TryGetValue(key, out value);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(KeyValuePair<TKey, TValue> kvp)
        {
            Add(kvp.Key, kvp.Value);
        }

        public void Add(TKey key, TValue value)
        {
            var valueNew = value.Clone();
            try
            {
                _dict.Add(key, valueNew);
            }
            catch 
            {
                valueNew.Dispose();
                throw;
            }
        }

        public bool TryAdd(ref TKey key, TValue value)
        {
            var valueNew = value.Clone();
            var res = _dict.TryAdd(key, valueNew);
            if (!res)
            {
                valueNew.Dispose();
            }
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Remove(KeyValuePair<TKey, TValue> kvp)
        {
            return Remove(kvp.Key);
        }

        public bool Remove(TKey key)
        {
            var res = _dict.Remove(key, out TValue value);
            if (res)
                value.Dispose();
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(KeyValuePair<TKey, TValue> kvp)
        {
            return _dict.Contains(kvp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<TKey, TValue>>)_dict).CopyTo(array, arrayIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _dict.GetEnumerator();
        }
    }
}
