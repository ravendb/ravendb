// PERF: Optimizations for CoreCLR standard Dictionary that require to change how the dictionary is
//       implemented. 


// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

/*============================================================
**
** 
** 
**
** Purpose: Generic hash table implementation
**
** #DictionaryVersusHashtableThreadSafety
** Hashtable has multiple reader/single writer (MR/SW) thread safety built into 
** certain methods and properties, whereas Dictionary doesn't. If you're 
** converting framework code that formerly used Hashtable to Dictionary, it's
** important to consider whether callers may have taken a dependence on MR/SW
** thread safety. If a reader writer lock is available, then that may be used
** with a Dictionary to get the same thread safety guarantee. 
** 
===========================================================*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

namespace Sparrow.Collections
{

    /// <summary>
    /// Used internally to control behavior of insertion into a <see cref="FastDictionary{TKey, TValue}"/>.
    /// </summary>
    internal enum InsertionBehavior : byte
    {
        /// <summary>
        /// The default insertion behavior.
        /// </summary>
        None = 0,

        /// <summary>
        /// Specifies that an existing entry with the same key should be overwritten if encountered.
        /// </summary>
        OverwriteExisting = 1,

        /// <summary>
        /// Specifies that if an existing entry with the same key is encountered, an exception should be thrown.
        /// </summary>
        ThrowOnExisting = 2
    }

    [DebuggerDisplay("Count = {Count}")]
    public sealed class FastDictionary<TKey, TValue, TComparer> : IEnumerable<KeyValuePair<TKey, TValue>> where TComparer : struct, IEqualityComparer<TKey>
    {
        private struct Entry
        {
            public int hashCode;    // Lower 31 bits of hash code, -1 if unused
            public int next;        // Index of next entry, -1 if last
            public TKey key;           // Key of entry
            public TValue value;         // Value of entry
        }

        private int[] buckets;
        private Entry[] entries;
        private int count;
        private int version;
        private int freeList;
        private int freeCount;
        private TComparer comparer;
        private KeyCollection keys;
        private ValueCollection values;
        private readonly object _syncRoot = new object();

        public FastDictionary() : this(0, default(TComparer)) { }

        public FastDictionary(int capacity) : this(capacity, default(TComparer)) { }

        public FastDictionary(TComparer comparer) : this(0, comparer) { }

        public FastDictionary(int capacity, TComparer comparer)
        {
            if (capacity < 0)
                ThrowArgumentOutOfRangeException(nameof(capacity));

            if (capacity > 0)
                Initialize(capacity);

            this.comparer = comparer;
        }

        public FastDictionary(IDictionary<TKey, TValue> dictionary) : this(dictionary, default(TComparer)) { }

        public FastDictionary(IDictionary<TKey, TValue> dictionary, TComparer comparer) :
            this(dictionary?.Count ?? 0, comparer)
        {
            if (dictionary == null)
            {
                ThrowArgumentNullException(nameof(dictionary));
            }

            foreach (KeyValuePair<TKey, TValue> pair in dictionary)
            {
                Add(pair.Key, pair.Value);
            }
        }

        public FastDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection) :
            this(collection, default(TComparer))
        { }

        public FastDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection, TComparer comparer) :
            this((collection as ICollection<KeyValuePair<TKey, TValue>>)?.Count ?? 0, comparer)
        {
            if (collection == null)
            {
                ThrowArgumentNullException(nameof(collection));
            }

            foreach (KeyValuePair<TKey, TValue> pair in collection)
            {
                Add(pair.Key, pair.Value);
            }
        }

        public IEqualityComparer<TKey> Comparer
        {
            get
            {
                return comparer;
            }
        }

        public int Capacity
        {
            get { return count; }
        }

        public int Count
        {
            get { return count - freeCount; }
        }

        public KeyCollection Keys
        {
            get
            {
                Contract.Ensures(Contract.Result<KeyCollection>() != null);
                if (keys == null)
                    keys = new KeyCollection(this);
                return keys;
            }
        }        

        public ValueCollection Values
        {
            get
            {
                Contract.Ensures(Contract.Result<ValueCollection>() != null);
                if (values == null)
                    values = new ValueCollection(this);
                return values;
            }
        }        

        public TValue this[TKey key]
        {
            get
            {
                int i = FindEntry(key);
                if (i >= 0)
                    return entries[i].value;

                ThrowKeyNotFoundException();
                return default(TValue);
            }
            set
            {
                bool modified = TryInsert(key, value, InsertionBehavior.OverwriteExisting);
                Debug.Assert(modified);
            }
        }

        public void Add(TKey key, TValue value)
        {
            bool modified = TryInsert(key, value, InsertionBehavior.ThrowOnExisting);
            Debug.Assert(modified); // If there was an existing key and the Add failed, an exception will already have been thrown.
        }        

        public void Clear()
        {
            if (count > 0)
            {
                for (int i = 0; i < buckets.Length; i++)
                    buckets[i] = -1;
                Array.Clear(entries, 0, count);
                freeList = -1;
                count = 0;
                freeCount = 0;
                version++;
            }
        }

        public bool ContainsKey(TKey key)
        {
            return FindEntry(key) >= 0;
        }

        public bool ContainsValue(TValue value)
        {
            if (value == null)
            {
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].hashCode >= 0 && entries[i].value == null)
                        return true;
                }
            }
            else
            {
                EqualityComparer<TValue> c = EqualityComparer<TValue>.Default;
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].hashCode >= 0 && c.Equals(entries[i].value, value))
                        return true;
                }
            }
            return false;
        }

        public Enumerator GetEnumerator()
        {
            return new Enumerator(this, Enumerator.KeyValuePair);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this, Enumerator.KeyValuePair);
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            return new Enumerator(this, Enumerator.KeyValuePair);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FindEntry(TKey key)
        {
            if (key == null)
            {
                ThrowArgumentNullException(nameof(key));
            }

            if (buckets != null)
            {
                int hashCode = comparer.GetHashCode(key) & 0x7FFFFFFF;
                for (int i = buckets[hashCode % buckets.Length]; i >= 0; i = entries[i].next)
                {
                    if (entries[i].hashCode == hashCode && comparer.Equals(entries[i].key, key))
                        return i;
                }
            }
            return -1;
        }

        private void Initialize(int capacity)
        {
            int size = HashHelpers.GetPrime(capacity);
            buckets = new int[size];
            for (int i = 0; i < buckets.Length; i++)
                buckets[i] = -1;
            entries = new Entry[size];
            freeList = -1;
        }

        private bool TryInsert(TKey key, TValue value, InsertionBehavior behavior)
        {
            if (key == null)
            {
                ThrowArgumentNullException(nameof(key));
            }

            if (buckets == null)
                Initialize(0);
            int hashCode = comparer.GetHashCode(key) & 0x7FFFFFFF;
            int targetBucket = hashCode % buckets.Length;

            for (int i = buckets[targetBucket]; i >= 0; i = entries[i].next)
            {
                if (entries[i].hashCode == hashCode && comparer.Equals(entries[i].key, key))
                {
                    if (behavior == InsertionBehavior.OverwriteExisting)
                    {
                        entries[i].value = value;
                        version++;
                        return true;
                    }

                    if (behavior == InsertionBehavior.ThrowOnExisting)
                    {
                        ThrowAddingDuplicateWithKeyArgumentException(key);
                    }

                    return false;
                }
            }
            int index;
            if (freeCount > 0)
            {
                index = freeList;
                freeList = entries[index].next;
                freeCount--;
            }
            else
            {
                if (count == entries.Length)
                {
                    Resize();
                    targetBucket = hashCode % buckets.Length;
                }
                index = count;
                count++;
            }

            entries[index].hashCode = hashCode;
            entries[index].next = buckets[targetBucket];
            entries[index].key = key;
            entries[index].value = value;
            buckets[targetBucket] = index;
            version++;

            return true;
        }

        private void Resize()
        {
            Resize(HashHelpers.ExpandPrime(count), false);
        }

        private void Resize(int newSize, bool forceNewHashCodes)
        {
            Debug.Assert(newSize >= entries.Length);
            int[] newBuckets = new int[newSize];
            for (int i = 0; i < newBuckets.Length; i++)
                newBuckets[i] = -1;
            Entry[] newEntries = new Entry[newSize];
            Array.Copy(entries, 0, newEntries, 0, count);
            if (forceNewHashCodes)
            {
                for (int i = 0; i < count; i++)
                {
                    if (newEntries[i].hashCode != -1)
                    {
                        newEntries[i].hashCode = (comparer.GetHashCode(newEntries[i].key) & 0x7FFFFFFF);
                    }
                }
            }
            for (int i = 0; i < count; i++)
            {
                if (newEntries[i].hashCode >= 0)
                {
                    int bucket = newEntries[i].hashCode % newSize;
                    newEntries[i].next = newBuckets[bucket];
                    newBuckets[bucket] = i;
                }
            }
            buckets = newBuckets;
            entries = newEntries;
        }

        // The overload Remove(TKey key, out TValue value) is a copy of this method with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.
        public bool Remove(TKey key)
        {
            if (key == null)
            {
                ThrowArgumentNullException(nameof(key));
            }

            if (buckets != null)
            {
                int hashCode = comparer.GetHashCode(key) & 0x7FFFFFFF;
                int bucket = hashCode % buckets.Length;
                int last = -1;
                int i = buckets[bucket];
                while (i >= 0)
                {
                    ref Entry entry = ref entries[i];

                    if (entry.hashCode == hashCode && comparer.Equals(entry.key, key))
                    {
                        if (last < 0)
                        {
                            buckets[bucket] = entry.next;
                        }
                        else
                        {
                            entries[last].next = entry.next;
                        }
                        entry.hashCode = -1;
                        entry.next = freeList;

                        entry.key = default(TKey);
                        entry.value = default(TValue);

                        //if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                        //{
                        //    entry.key = default(TKey);
                        //}
                        //if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                        //{
                        //    entry.value = default(TValue);
                        //}

                        freeList = i;
                        freeCount++;
                        version++;
                        return true;
                    }

                    last = i;
                    i = entry.next;
                }
            }
            return false;
        }

        // This overload is a copy of the overload Remove(TKey key) with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.
        public bool Remove(TKey key, out TValue value)
        {
            if (key == null)
            {
                ThrowArgumentNullException(nameof(key));
            }

            if (buckets != null)
            {
                int hashCode = comparer.GetHashCode(key) & 0x7FFFFFFF;
                int bucket = hashCode % buckets.Length;
                int last = -1;
                int i = buckets[bucket];
                while (i >= 0)
                {
                    ref Entry entry = ref entries[i];

                    if (entry.hashCode == hashCode && comparer.Equals(entry.key, key))
                    {
                        if (last < 0)
                        {
                            buckets[bucket] = entry.next;
                        }
                        else
                        {
                            entries[last].next = entry.next;
                        }

                        value = entry.value;

                        entry.hashCode = -1;
                        entry.next = freeList;
                        entry.key = default(TKey);
                        entry.value = default(TValue);

                        //if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                        //{
                        //    entry.key = default(TKey);
                        //}
                        //if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                        //{
                        //    entry.value = default(TValue);
                        //}

                        freeList = i;
                        freeCount++;
                        version++;
                        return true;
                    }

                    last = i;
                    i = entry.next;
                }
            }
            value = default(TValue);
            return false;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            int i = FindEntry(key);
            if (i >= 0)
            {
                value = entries[i].value;
                return true;
            }
            value = default(TValue);
            return false;
        }

        public bool TryAdd(TKey key, TValue value) => TryInsert(key, value, InsertionBehavior.None);        

        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>,
            IDictionaryEnumerator
        {
            private readonly FastDictionary<TKey, TValue, TComparer> _dictionary;
            private readonly int _version;
            private int _index;
            private KeyValuePair<TKey, TValue> _current;
            private readonly int _getEnumeratorRetType;  // What should Enumerator.Current return?

            internal const int DictEntry = 1;
            internal const int KeyValuePair = 2;

            internal Enumerator(FastDictionary<TKey, TValue, TComparer> dictionary, int getEnumeratorRetType)
            {
                _dictionary = dictionary;
                _version = dictionary.version;
                _index = 0;
                _getEnumeratorRetType = getEnumeratorRetType;
                _current = new KeyValuePair<TKey, TValue>();
            }

            public bool MoveNext()
            {
                if (_version != _dictionary.version)
                {
                    ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                // Use unsigned comparison since we set index to dictionary.count+1 when the enumeration ends.
                // dictionary.count+1 could be negative if dictionary.count is Int32.MaxValue
                while ((uint)_index < (uint)_dictionary.count)
                {
                    ref Entry entry = ref _dictionary.entries[_index++];

                    if (entry.hashCode >= 0)
                    {
                        _current = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
                        return true;
                    }
                }

                _index = _dictionary.count + 1;
                _current = new KeyValuePair<TKey, TValue>();
                return false;
            }

            public KeyValuePair<TKey, TValue> Current
            {
                get { return _current; }
            }

            public void Dispose()
            {
            }

            object IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary.count + 1))
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    if (_getEnumeratorRetType == DictEntry)
                    {
                        return new DictionaryEntry(_current.Key, _current.Value);
                    }
                    else
                    {
                        return new KeyValuePair<TKey, TValue>(_current.Key, _current.Value);
                    }
                }
            }

            void IEnumerator.Reset()
            {
                if (_version != _dictionary.version)
                {
                    ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                _index = 0;
                _current = new KeyValuePair<TKey, TValue>();
            }

            DictionaryEntry IDictionaryEnumerator.Entry
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary.count + 1))
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return new DictionaryEntry(_current.Key, _current.Value);
                }
            }

            object IDictionaryEnumerator.Key
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary.count + 1))
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _current.Key;
                }
            }

            object IDictionaryEnumerator.Value
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary.count + 1))
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _current.Value;
                }
            }
        }

        [DebuggerDisplay("Count = {Count}")]
        public sealed class KeyCollection : ICollection<TKey>, ICollection, IReadOnlyCollection<TKey>
        {
            private readonly FastDictionary<TKey, TValue, TComparer> _dictionary;

            public KeyCollection(FastDictionary<TKey, TValue, TComparer> dictionary)
            {
                if (dictionary == null)
                {
                    ThrowArgumentNullException(nameof(dictionary));
                }

                _dictionary = dictionary;
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            public void CopyTo(TKey[] array, int index)
            {
                if (array == null)
                {
                    ThrowArgumentNullException(nameof(array));
                }

                if (index < 0 || index > array.Length)
                {
                    ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
                }

                if (array.Length - index < _dictionary.Count)
                {
                    ThrowArgumentException("ExceptionResource.Arg_ArrayPlusOffTooSmall");
                }

                int count = _dictionary.count;
                Entry[] entries = _dictionary.entries;
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].hashCode >= 0)
                        array[index++] = entries[i].key;
                }
            }

            public int Count
            {
                get { return _dictionary.Count; }
            }

            bool ICollection<TKey>.IsReadOnly
            {
                get { return true; }
            }

            void ICollection<TKey>.Add(TKey item)
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_KeyCollectionSet");
            }

            void ICollection<TKey>.Clear()
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_KeyCollectionSet");
            }

            bool ICollection<TKey>.Contains(TKey item)
            {
                return _dictionary.ContainsKey(item);
            }

            bool ICollection<TKey>.Remove(TKey item)
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_KeyCollectionSet");
                return false;
            }

            IEnumerator<TKey> IEnumerable<TKey>.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            void ICollection.CopyTo(Array array, int index)
            {
                if (array == null)
                {
                    ThrowArgumentNullException(nameof(array));
                }

                if (array.Rank != 1)
                {
                    ThrowArgumentException("ExceptionResource.Arg_RankMultiDimNotSupported");
                }

                if (array.GetLowerBound(0) != 0)
                {
                    ThrowArgumentException("ExceptionResource.Arg_NonZeroLowerBound");
                }

                if (index < 0 || index > array.Length)
                {
                    ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
                }

                if (array.Length - index < _dictionary.Count)
                {
                    ThrowArgumentException("ExceptionResource.Arg_ArrayPlusOffTooSmall");
                }

                if (array is TKey[] keys)
                {
                    CopyTo(keys, index);
                }
                else
                {
                    object[] objects = array as object[];
                    if (objects == null)
                    {
                        ThrowArgumentException_Argument_InvalidArrayType();
                    }

                    int count = _dictionary.count;
                    Entry[] entries = _dictionary.entries;
                    try
                    {
                        for (int i = 0; i < count; i++)
                        {
                            if (entries[i].hashCode >= 0)
                                objects[index++] = entries[i].key;
                        }
                    }
                    catch (ArrayTypeMismatchException)
                    {
                        ThrowArgumentException_Argument_InvalidArrayType();
                    }
                }
            }

            bool ICollection.IsSynchronized
            {
                get { return false; }
            }

            Object ICollection.SyncRoot
            {
                get { return _dictionary._syncRoot; }
            }

            public struct Enumerator : IEnumerator<TKey>
            {
                private readonly FastDictionary<TKey, TValue, TComparer> _dictionary;
                private int _index;
                private readonly int version;
                private TKey _currentKey;

                internal Enumerator(FastDictionary<TKey, TValue, TComparer> dictionary)
                {
                    _dictionary = dictionary;
                    version = dictionary.version;
                    _index = 0;
                    _currentKey = default(TKey);
                }

                public void Dispose()
                {
                }

                public bool MoveNext()
                {
                    if (version != _dictionary.version)
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                    }

                    while ((uint)_index < (uint)_dictionary.count)
                    {
                        ref Entry entry = ref _dictionary.entries[_index++];

                        if (entry.hashCode >= 0)
                        {
                            _currentKey = entry.key;
                            return true;
                        }
                    }

                    _index = _dictionary.count + 1;
                    _currentKey = default(TKey);
                    return false;
                }

                public TKey Current
                {
                    get
                    {
                        return _currentKey;
                    }
                }

                Object IEnumerator.Current
                {
                    get
                    {
                        if (_index == 0 || (_index == _dictionary.count + 1))
                        {
                            ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                        }

                        return _currentKey;
                    }
                }

                void IEnumerator.Reset()
                {
                    if (version != _dictionary.version)
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                    }

                    _index = 0;
                    _currentKey = default(TKey);
                }
            }
        }

        [DebuggerDisplay("Count = {Count}")]
        public sealed class ValueCollection : ICollection<TValue>, ICollection, IReadOnlyCollection<TValue>
        {
            private readonly FastDictionary<TKey, TValue, TComparer> _dictionary;

            public ValueCollection(FastDictionary<TKey, TValue, TComparer> dictionary)
            {
                if (dictionary == null)
                {
                    ThrowArgumentNullException(nameof(dictionary));
                }

                _dictionary = dictionary;
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            public void CopyTo(TValue[] array, int index)
            {
                if (array == null)
                {
                    ThrowArgumentNullException(nameof(array));
                }

                if (index < 0 || index > array.Length)
                {
                    ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
                }

                if (array.Length - index < _dictionary.Count)
                {
                    ThrowArgumentException("ExceptionResource.Arg_ArrayPlusOffTooSmall");
                }

                int count = _dictionary.count;
                Entry[] entries = _dictionary.entries;
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].hashCode >= 0)
                        array[index++] = entries[i].value;
                }
            }

            public int Count
            {
                get { return _dictionary.Count; }
            }

            bool ICollection<TValue>.IsReadOnly
            {
                get { return true; }
            }

            void ICollection<TValue>.Add(TValue item)
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_ValueCollectionSet");
            }

            bool ICollection<TValue>.Remove(TValue item)
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_ValueCollectionSet");
                return false;
            }

            void ICollection<TValue>.Clear()
            {
                ThrowNotSupportedException("ExceptionResource.NotSupported_ValueCollectionSet");
            }

            bool ICollection<TValue>.Contains(TValue item)
            {
                return _dictionary.ContainsValue(item);
            }

            IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            void ICollection.CopyTo(Array array, int index)
            {
                if (array == null)
                {
                    ThrowArgumentNullException(nameof(array));
                }

                if (array.Rank != 1)
                {
                    ThrowArgumentException("ExceptionResource.Arg_RankMultiDimNotSupported");
                }

                if (array.GetLowerBound(0) != 0)
                {
                    ThrowArgumentException("ExceptionResource.Arg_NonZeroLowerBound");
                }

                if (index < 0 || index > array.Length)
                {
                    ThrowIndexArgumentOutOfRange_NeedNonNegNumException();
                }

                if (array.Length - index < _dictionary.Count)
                    ThrowArgumentException("ExceptionResource.Arg_ArrayPlusOffTooSmall");

                if (array is TValue[] values)
                {
                    CopyTo(values, index);
                }
                else
                {
                    object[] objects = array as object[];
                    if (objects == null)
                    {
                        ThrowArgumentException_Argument_InvalidArrayType();
                    }

                    int count = _dictionary.count;
                    Entry[] entries = _dictionary.entries;
                    try
                    {
                        for (int i = 0; i < count; i++)
                        {
                            if (entries[i].hashCode >= 0)
                                objects[index++] = entries[i].value;
                        }
                    }
                    catch (ArrayTypeMismatchException)
                    {
                        ThrowArgumentException_Argument_InvalidArrayType();
                    }
                }
            }

            bool ICollection.IsSynchronized
            {
                get { return false; }
            }

            Object ICollection.SyncRoot
            {
                get { return _dictionary._syncRoot; }
            }

            public struct Enumerator : IEnumerator<TValue>, IEnumerator
            {
                private readonly FastDictionary<TKey, TValue, TComparer> _dictionary;
                private int _index;
                private readonly int _version;
                private TValue _currentValue;

                internal Enumerator(FastDictionary<TKey, TValue, TComparer> dictionary)
                {
                    _dictionary = dictionary;
                    _version = dictionary.version;
                    _index = 0;
                    _currentValue = default(TValue);
                }

                public void Dispose()
                {
                }

                public bool MoveNext()
                {
                    if (_version != _dictionary.version)
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                    }

                    while ((uint)_index < (uint)_dictionary.count)
                    {
                        ref Entry entry = ref _dictionary.entries[_index++];

                        if (entry.hashCode >= 0)
                        {
                            _currentValue = entry.value;
                            return true;
                        }
                    }
                    _index = _dictionary.count + 1;
                    _currentValue = default(TValue);
                    return false;
                }

                public TValue Current
                {
                    get
                    {
                        return _currentValue;
                    }
                }

                Object IEnumerator.Current
                {
                    get
                    {
                        if (_index == 0 || (_index == _dictionary.count + 1))
                        {
                            ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                        }

                        return _currentValue;
                    }
                }

                void IEnumerator.Reset()
                {
                    if (_version != _dictionary.version)
                    {
                        ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                    }
                    _index = 0;
                    _currentValue = default(TValue);
                }
            }
        }

        private static void ThrowArgumentOutOfRangeException(string param)
        {
            throw new ArgumentOutOfRangeException(param);
        }

        private static void ThrowArgumentNullException(string param)
        {
            throw new ArgumentNullException(param);
        }

        private static void ThrowKeyNotFoundException()
        {
            throw new ArgumentException("Key not found.");
        }

        private static void ThrowArgumentException(string param)
        {
            throw new ArgumentException(param);
        }

        private static void ThrowIndexArgumentOutOfRange_NeedNonNegNumException()
        {
            throw new ArgumentOutOfRangeException("Need non negative number as index");
        }

        private static void ThrowAddingDuplicateWithKeyArgumentException(TKey key)
        {
            throw new ArgumentException("Cannot add duplicate key", nameof(key));
        }

        private static void ThrowNotSupportedException(string msg)
        {
            throw new NotSupportedException(msg);
        }

        private static void ThrowArgumentException_Argument_InvalidArrayType()
        {
            throw new ArgumentException("Invalid array type");
        }

        private static void ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen()
        {
            throw new InvalidOperationException("Enum value cant happen");
        }

        private static void ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion()
        {
            throw new InvalidOperationException("Enum failed version");
        }

        internal static class HashHelpers
        {
            public const int HashCollisionThreshold = 100;

            // Table of prime numbers to use as hash table sizes. 
            // A typical resize algorithm would pick the smallest prime number in this array
            // that is larger than twice the previous capacity. 
            // Suppose our Hashtable currently has capacity x and enough elements are added 
            // such that a resize needs to occur. Resizing first computes 2x then finds the 
            // first prime in the table greater than 2x, i.e. if primes are ordered 
            // p_1, p_2, ..., p_i, ..., it finds p_n such that p_n-1 < 2x < p_n. 
            // Doubling is important for preserving the asymptotic complexity of the 
            // hashtable operations such as add.  Having a prime guarantees that double 
            // hashing does not lead to infinite loops.  IE, your hash function will be 
            // h1(key) + i*h2(key), 0 <= i < size.  h2 and the size must be relatively prime.
            public static readonly int[] primes = {
            3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
            1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
            17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
            187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
            1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369};

            internal const int HashPrime = 101;

            public static bool IsPrime(int candidate)
            {
                if ((candidate & 1) != 0)
                {
                    int limit = (int)Math.Sqrt(candidate);
                    for (int divisor = 3; divisor <= limit; divisor += 2)
                    {
                        if ((candidate % divisor) == 0)
                            return false;
                    }
                    return true;
                }
                return (candidate == 2);
            }

            public static int GetPrime(int min)
            {
                if (min < 0)
                    throw new ArgumentException(nameof(min));

                Contract.EndContractBlock();

                for (int i = 0; i < primes.Length; i++)
                {
                    int prime = primes[i];
                    if (prime >= min)
                        return prime;
                }

                //outside of our predefined table. 
                //compute the hard way. 
                for (int i = (min | 1); i < Int32.MaxValue; i += 2)
                {
                    if (IsPrime(i) && ((i - 1) % HashPrime != 0))
                        return i;
                }
                return min;
            }

            // Returns size of hashtable to grow to.
            public static int ExpandPrime(int oldSize)
            {
                int newSize = 2 * oldSize;

                // Allow the hashtables to grow to maximum possible size (~2G elements) before encoutering capacity overflow.
                // Note that this check works even when _items.Length overflowed thanks to the (uint) cast
                if ((uint)newSize > MaxPrimeArrayLength && MaxPrimeArrayLength > oldSize)
                {
                    Debug.Assert(MaxPrimeArrayLength == GetPrime(MaxPrimeArrayLength), "Invalid MaxPrimeArrayLength");
                    return MaxPrimeArrayLength;
                }

                return GetPrime(newSize);
            }


            // This is the maximum prime smaller than Array.MaxArrayLength
            public const int MaxPrimeArrayLength = 0x7FEFFFFD;
        }
    }
}
