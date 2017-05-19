// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Sparrow.Collections.LockFree.DictionaryImpl;

namespace Sparrow.Collections.LockFree
{
    public sealed class ConcurrentDictionary<TKey, TValue> :
        IDictionary<TKey, TValue>,
        IReadOnlyDictionary<TKey, TValue>,
        IDictionary,
        ICollection
    {
        internal DictionaryImpl<TKey, TValue> _table;
        internal uint _lastResizeTickMillis;

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that is empty, has the default concurrency level, has the default initial capacity, and uses the default comparer for the key type.</summary>
        public ConcurrentDictionary() : this(31)
        {
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <param name="capacity">The initial number of elements that the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> can contain.</param>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that is empty, has the default concurrency level and uses the default comparer for the key type.</summary>
        public ConcurrentDictionary(int capacity) : this(capacity, null)
        {
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that is empty, has the specified concurrency level and capacity, and uses the default comparer for the key type.</summary>
        /// <param name="concurrencyLevel">The estimated number of threads that will update the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> concurrently.</param>
        /// <param name="capacity">The initial number of elements that the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> can contain.</param>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        ///   <paramref name="concurrencyLevel" /> is less than 1.-or-<paramref name="capacity" /> is less than 0.</exception>
        public ConcurrentDictionary(int concurrencyLevel, int capacity) : this(capacity)
        {
            if (concurrencyLevel < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(concurrencyLevel));
            }
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that contains elements copied from the specified <see cref="T:System.Collections.IEnumerable{KeyValuePair{TKey,TValue}}" />, has the default concurrency level, has the default initial capacity, and uses the default comparer for the key type.</summary>
        /// <param name="collection">The <see cref="T:System.Collections.IEnumerable{KeyValuePair{TKey,TValue}}" /> whose elements are copied to the new <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" />.</param>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="collection" /> or any of its keys is a null reference (Nothing in Visual Basic)</exception>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="collection" /> contains one or more duplicate keys.</exception>
        public ConcurrentDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection) : this()
        {
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection));
            }
            this.InitializeFromCollection(collection);
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that is empty, has the default concurrency level and capacity, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" />.</summary>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" /> implementation to use when comparing keys.</param>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="comparer" /> is a null reference (Nothing in Visual Basic).</exception>
        public ConcurrentDictionary(IEqualityComparer<TKey> comparer) : this(31, comparer)
        {
            if (comparer == null)
            {
                throw new ArgumentNullException(nameof(comparer));
            }
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that contains elements copied from the specified <see cref="T:System.Collections.IEnumerable" />, has the default concurrency level, has the default initial capacity, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" />.</summary>
        /// <param name="collection">The <see cref="T:System.Collections.IEnumerable{KeyValuePair{TKey,TValue}}" /> whose elements are copied to the new <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" />.</param>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" /> implementation to use when comparing keys.</param>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="collection" /> is a null reference (Nothing in Visual Basic). -or- <paramref name="comparer" /> is a null reference (Nothing in Visual Basic).</exception>
        public ConcurrentDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection, IEqualityComparer<TKey> comparer) : this(comparer)
        {
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection));
            }
            if (comparer == null)
            {
                throw new ArgumentNullException(nameof(comparer));
            }
            this.InitializeFromCollection(collection);
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that contains elements copied from the specified <see cref="T:System.Collections.IEnumerable" />, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" />.</summary>
        /// <param name="concurrencyLevel">The estimated number of threads that will update the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> concurrently.</param>
        /// <param name="collection">The <see cref="T:System.Collections.IEnumerable{KeyValuePair{TKey,TValue}}" /> whose elements are copied to the new <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" />.</param>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" /> implementation to use when comparing keys.</param>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="collection" /> is a null reference (Nothing in Visual Basic). -or- <paramref name="comparer" /> is a null reference (Nothing in Visual Basic).</exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        ///   <paramref name="concurrencyLevel" /> is less than 1.</exception>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="collection" /> contains one or more duplicate keys.</exception>
        public ConcurrentDictionary(int concurrencyLevel, IEnumerable<KeyValuePair<TKey, TValue>> collection, IEqualityComparer<TKey> comparer) : this(comparer)
        {
            if (concurrencyLevel < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(concurrencyLevel));
            }
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection));
            }
            if (comparer == null)
            {
                throw new ArgumentNullException(nameof(comparer));
            }
            this.InitializeFromCollection(collection);
        }

        private void InitializeFromCollection(IEnumerable<KeyValuePair<TKey, TValue>> collection)
        {
            foreach (KeyValuePair<TKey, TValue> current in collection)
            {
                if (!this.TryAdd(current.Key, current.Value))
                {
                    throw new ArgumentException("Collection contains duplicate keys");
                }
            }
        }

        // System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue>
        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> class that is empty, has the specified concurrency level, has the specified initial capacity, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" />.</summary>
        /// <param name="concurrencyLevel">The estimated number of threads that will update the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> concurrently.</param>
        /// <param name="capacity">The initial number of elements that the <see cref="T:System.Collections.Concurrent.ConcurrentDictionary`2" /> can contain.</param>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer{TKey}" /> implementation to use when comparing keys.</param>
        /// <exception cref="T:System.ArgumentNullException">
        ///   <paramref name="comparer" /> is a null reference (Nothing in Visual Basic).</exception>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        ///   <paramref name="concurrencyLevel" /> is less than 1. -or- <paramref name="capacity" /> is less than 0.</exception>
        public ConcurrentDictionary(int concurrencyLevel, int capacity, IEqualityComparer<TKey> comparer) : this(capacity, comparer)
        {
            if (concurrencyLevel < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(concurrencyLevel));
            }
            if (comparer == null)
            {
                throw new ArgumentNullException(nameof(comparer));
            }
        }

        private ConcurrentDictionary(
            int capacity,
            IEqualityComparer<TKey> comparer = null)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            if (default(TKey) == null)
            {
                if (typeof(TKey) == typeof(ValueType) ||
                    !(default(TKey) is ValueType))
                {
                    _table = DictionaryImpl<TKey, TValue>.CreateRefUnsafe(this, capacity);
                    _table._keyComparer = comparer ?? EqualityComparer<TKey>.Default;
                    return;
                }
            }
            else
            {
                if (typeof(TKey) == typeof(uint) || typeof(TKey) == typeof(ulong))
                    throw new NotSupportedException("Unsupported until we have confirmation of how to by-pass the code-gen issue with the casting of Boxed<TKey>. Use int or long instead.");

                if (typeof(TKey) == typeof(int))
                {
                    if (comparer == null)
                    {
                        _table = (DictionaryImpl<TKey, TValue>)(object)new DictionaryImplIntNoComparer<TValue>(capacity, (ConcurrentDictionary<int, TValue>)(object)this);
                    }
                    else
                    {
                        _table = (DictionaryImpl<TKey, TValue>)(object)new DictionaryImplInt<TValue>(capacity, (ConcurrentDictionary<int, TValue>)(object)this);
                        _table._keyComparer = comparer;
                    }
                    return;
                }

                if (typeof(TKey) == typeof(long))
                {
                    if (comparer == null)
                    {
                        _table = (DictionaryImpl<TKey, TValue>)(object)new DictionaryImplLongNoComparer<TValue>(capacity, (ConcurrentDictionary<long, TValue>)(object)this);
                    }
                    else
                    {
                        _table = (DictionaryImpl<TKey, TValue>)(object)new DictionaryImplLong<TValue>(capacity, (ConcurrentDictionary<long, TValue>)(object)this);
                        _table._keyComparer = comparer;
                    }
                    return ;
                }
            }

            _table = new DictionaryImplBoxed<TKey, TValue>(capacity, this)
            {
                _keyComparer = comparer ?? EqualityComparer<TKey>.Default
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(TKey key, TValue value)
        {
            if (!TryAdd(key, value))
            {
                throw new ArgumentException("AddingDuplicate");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(TKey key, TValue value)
        {
            object oldValObj = null;
            object newValObj = ToObjectValue(value);
            return _table.PutIfMatch(key, newValObj, ref oldValObj, ValueMatch.NullOrDead);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Remove(TKey key)
        {
            object oldValObj = null;
            var found = _table.PutIfMatch(key, TOMBSTONE, ref oldValObj, ValueMatch.NotNullOrDead);
            Debug.Assert(!(oldValObj is Prime));

            return found;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRemove(TKey key, out TValue value)
        {
            object oldValObj = null;
            var found = _table.PutIfMatch(key, TOMBSTONE, ref oldValObj, ValueMatch.NotNullOrDead);

            Debug.Assert(!(oldValObj is Prime));
            Debug.Assert(found ^ oldValObj == null);

            // PERF: this would be nice to have as a helper, 
            // but it does not get inlined
            if (default(TValue) == null && oldValObj == NULLVALUE)
            {
                oldValObj = null;
            }

            value = found ?
                (TValue)oldValObj :
                default(TValue);

            return found;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(TKey key, out TValue value)
        {
            object oldValObj = _table.TryGetValue(key);

            Debug.Assert(!(oldValObj is Prime));

            if (oldValObj != null)
            {
                // PERF: this would be nice to have as a helper, 
                // but it does not get inlined
                if (default(TValue) == null && oldValObj == NULLVALUE)
                {
                    value = default(TValue);
                }
                else
                {
                    value = (TValue)oldValObj;
                }
                return true;
            }

            value = default(TValue);
            return false;
        }


        public TValue this[TKey key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                object oldValObj = _table.TryGetValue(key);

                Debug.Assert(!(oldValObj is Prime));

                if (oldValObj != null)
                {
                    // PERF: this would be nice to have as a helper, 
                    // but it does not get inlined
                    TValue value;
                    if (default(TValue) == null && oldValObj == NULLVALUE)
                    {
                        value = default(TValue);
                    }
                    else
                    {
                        value = (TValue)oldValObj;
                    }

                    return value;
                }

                return ThrowKeyNotFound();
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                object oldValObj = null;
                object newValObj = ToObjectValue(value);
                _table.PutIfMatch(key, newValObj, ref oldValObj, ValueMatch.Any);
            }
        }

        private TValue ThrowKeyNotFound()
        {
            throw new KeyNotFoundException();
        }

        public bool ContainsKey(TKey key)
        {
            TValue value;
            return this.TryGetValue(key, out value);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        public bool Contains(KeyValuePair<TKey, TValue> keyValuePair)
        {
            TValue value;
            return TryGetValue(keyValuePair.Key, out value) && 
                EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdate(TKey key, TValue value, TValue comparisonValue)
        {
            object oldValObj = ToObjectValue(comparisonValue);
            object newValObj = ToObjectValue(value);
            return _table.PutIfMatch(key, newValObj, ref oldValObj, ValueMatch.OldValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, TValue value)
        {
            object oldValObj = null;
            object newValObj = ToObjectValue(value);
            if (_table.PutIfMatch(key, newValObj, ref oldValObj, ValueMatch.NullOrDead))
            {
                return value;
            }

            // PERF: this would be nice to have as a helper, 
            // but it does not get inlined
            if (default(TValue) == null && oldValObj == NULLVALUE)
            {
                oldValObj = null;
            }

            return (TValue)oldValObj;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            return _table.GetOrAdd(key, valueFactory);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            object oldValObj = ToObjectValue(item.Value);
            return _table.PutIfMatch(item.Key, TOMBSTONE, ref oldValObj, ValueMatch.OldValue);
        }

        bool IDictionary.IsReadOnly => false;
        bool IDictionary.IsFixedSize => false;
        bool ICollection.IsSynchronized => false;
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        public bool IsEmpty => _table.Count == 0;
        public int Count => _table.Count;
        public void Clear() => _table.Clear();

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;
        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;
        ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;
        ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;
        ICollection IDictionary.Keys => Keys;
        ICollection IDictionary.Values => Values;

        bool IDictionary.Contains(object key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            return key is TKey && this.ContainsKey((TKey)((object)key));
        }

        void IDictionary.Add(object key, object value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (!(key is TKey))
            {
                throw new ArgumentException();
            }
            TValue value2;
            try
            {
                value2 = (TValue)((object)value);
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException();
            }
            ((IDictionary<TKey, TValue>)this).Add((TKey)((object)key), value2);
        }

        void IDictionary.Remove(object key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (key is TKey)
            {
                TValue tValue;
                this.TryRemove((TKey)((object)key), out tValue);
            }
        }

        void ICollection.CopyTo(Array array, int index)
        {
            var pairs = array as KeyValuePair<TKey, TValue>[];
            if (pairs != null)
            {
                CopyTo(pairs, index);
                return;
            }

            var entries = array as DictionaryEntry[];
            if (entries != null)
            {
                CopyTo(entries, index);
                return;
            }

            var objects = array as object[];
            if (objects != null)
            {
                CopyTo(objects, index);
                return;
            }

            throw new ArgumentNullException(nameof(array));
        }

        public object SyncRoot => throw new NotSupportedException();

        object IDictionary.this[object key]
        {
            get
            {
                if (key == null)
                {
                    throw new ArgumentNullException(nameof(key));
                }
                TValue tValue;
                if (key is TKey && this.TryGetValue((TKey)((object)key), out tValue))
                {
                    return tValue;
                }
                return null;
            }
            set
            {
                if (key == null)
                {
                    throw new ArgumentNullException(nameof(key));
                }
                if (!(key is TKey))
                {
                    throw new ArgumentException();
                }
                if (!(value is TValue))
                {
                    throw new ArgumentException();
                }
                this[(TKey)((object)key)] = (TValue)((object)value);
            }
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (addValueFactory == null)
            {
                throw new ArgumentNullException(nameof(addValueFactory));
            }
            if (updateValueFactory == null)
            {
                throw new ArgumentNullException(nameof(updateValueFactory));
            }
            TValue tValue2;
            while (true)
            {
                TValue tValue;
                if (this.TryGetValue(key, out tValue))
                {
                    tValue2 = updateValueFactory(key, tValue);
                    if (this.TryUpdate(key, tValue2, tValue))
                    {
                        break;
                    }
                }
                else
                {
                    tValue2 = addValueFactory(key);
                    if (this.TryAdd(key, tValue2))
                    {
                        break;
                    }
                }
            }
            return tValue2;
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (updateValueFactory == null)
            {
                throw new ArgumentNullException(nameof(updateValueFactory));
            }
            while (true)
            {
                TValue tValue;
                if (this.TryGetValue(key, out tValue))
                {
                    TValue tValue2 = updateValueFactory(key, tValue);
                    if (this.TryUpdate(key, tValue2, tValue))
                    {
                        return tValue2;
                    }
                }
                else if (this.TryAdd(key, addValue))
                {
                    return addValue;
                }
            }            
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("index");
            }

            foreach (var entry in this)
            {
                array[arrayIndex++] = entry;
            }
        }

        public void CopyTo(DictionaryEntry[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("array");
            }

            foreach (var entry in this)
            {
                array[arrayIndex++] = new DictionaryEntry(entry.Key, entry.Value);
            }
        }

        public void CopyTo(object[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("index");
            }

            var length = array.Length;
            foreach (var entry in this)
            {
                if ((uint)arrayIndex < (uint)length)
                {
                    array[arrayIndex++] = entry;
                }
                else
                {
                    throw new ArgumentException();
                }
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _table.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _table.GetEnumerator();
        }

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return _table.GetDictionaryEnumerator();
        }

        public ReadOnlyCollection<TKey> Keys
        {
            get
            {
                var keys = new List<TKey>(Count);
                foreach (var kv in this)
                {
                    keys.Add(kv.Key);
                }

                return new ReadOnlyCollection<TKey>(keys);
            }
        }

        public ReadOnlyCollection<TValue> Values
        {
            get
            {
                var values = new List<TValue>(Count);
                foreach (var kv in this)
                {
                    values.Add(kv.Value);
                }

                return new ReadOnlyCollection<TValue>(values);
            }
        }
    }
}
