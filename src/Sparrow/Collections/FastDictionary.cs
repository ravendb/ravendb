using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using Sparrow.Binary;

namespace Sparrow.Collections
{
    internal static class DictionaryHelper
    {
        /// <summary>
        /// Minimum size we're willing to let hashtables be.
        /// Must be a power of two, and at least 4.
        /// Note, however, that for a given hashtable, the initial size is a function of the first constructor arg, and may be > kMinBuckets.
        /// </summary>
        internal const int KMinBuckets = 4;

        /// <summary>
        /// By default, if you don't specify a hashtable size at construction-time, we use this size.  Must be a power of two, and at least kMinBuckets.
        /// </summary>
        internal const int KInitialCapacity = 32;
    }

    // PERF: CoreCLR 2.0 JIT will pickup this is a sealed class and will try to devirtualize all method calls to comparers.
    public sealed class FastDictionary<TKey, TValue> : FastDictionaryBase<TKey, TValue, IEqualityComparer<TKey>>
    {
        public FastDictionary(int initialBucketCount, IEnumerable<KeyValuePair<TKey, TValue>> src, IEqualityComparer<TKey> comparer) : base(initialBucketCount, src, comparer ?? EqualityComparer<TKey>.Default)
        { }

        public FastDictionary(FastDictionary<TKey, TValue> src, IEqualityComparer<TKey> comparer) : base(src, comparer ?? EqualityComparer<TKey>.Default)
        { }

        public FastDictionary(FastDictionary<TKey, TValue> src) : base(src)
        { }

        public FastDictionary(int initialBucketCount, FastDictionary<TKey, TValue> src, IEqualityComparer<TKey> comparer) : base(initialBucketCount, src, comparer ?? EqualityComparer<TKey>.Default)
        { }

        public FastDictionary(IEqualityComparer<TKey> comparer) : base(comparer ?? EqualityComparer<TKey>.Default)
        { }

        public FastDictionary(int initialBucketCount, IEqualityComparer<TKey> comparer) : base(initialBucketCount, comparer ?? EqualityComparer<TKey>.Default)
        { }

        public FastDictionary(int initialBucketCount = DictionaryHelper.KInitialCapacity) : base(initialBucketCount, EqualityComparer<TKey>.Default)
        { }
    }

    // PERF: CoreCLR 2.0 JIT will pickup this is a sealed class and will try to devirtualize all method calls to comparers.
    public sealed class FastDictionary<TKey, TValue, TComparer> : FastDictionaryBase<TKey, TValue, TComparer>
        where TComparer : IEqualityComparer<TKey>
    {
        public FastDictionary(int initialBucketCount, IEnumerable<KeyValuePair<TKey, TValue>> src, TComparer comparer) : base(initialBucketCount, src, comparer)
        { }

        public FastDictionary(FastDictionary<TKey, TValue, TComparer> src, TComparer comparer) : base(src, comparer)
        { }

        public FastDictionary(FastDictionary<TKey, TValue, TComparer> src) : base(src)
        { }

        public FastDictionary(int initialBucketCount, FastDictionary<TKey, TValue, TComparer> src, TComparer comparer) : base(initialBucketCount, src, comparer)
        { }

        public FastDictionary(TComparer comparer) : base(comparer)
        { }

        public FastDictionary(int initialBucketCount, TComparer comparer) : base(initialBucketCount, comparer)
        { }
    }

    // PERF: This base class is introduced in order to allow generic specialization.
    public abstract class FastDictionaryBase<TKey, TValue, TComparer> : IEnumerable<KeyValuePair<TKey, TValue>>
        where TComparer : IEqualityComparer<TKey>
    {
        const int InvalidNodePosition = -1;

        public const uint KUnusedHash = 0xFFFFFFFF;
        public const uint KDeletedHash = 0xFFFFFFFE;

        // TLoadFactor4 - controls hash map load. 4 means 100% load, ie. hashmap will grow
        // when number of items == capacity. Default value of 6 means it grows when
        // number of items == capacity * 3/2 (6/4). Higher load == tighter maps, but bigger
        // risk of collisions.
        private const int KLoadFactor = 6;

        private struct Entry
        {
            public uint Hash;
            public TKey Key;
            public TValue Value;

            public Entry ( uint hash, TKey key, TValue value)
            {
                this.Hash = hash;
                this.Key = key;
                this.Value = value;
            }
        }

        private Entry[] _entries;

        private readonly int _initialCapacity; // This is the initial capacity of the dictionary, we will never shrink beyond this point.
        private int _capacity;
        private int _capacityMask;
        
        private int _size; // This is the real counter of how many items are in the hash-table (regardless of buckets)
        private int _numberOfUsed; // How many used buckets. 
        private int _numberOfDeleted; // how many occupied buckets are marked deleted
        private int _nextGrowthThreshold;


        // PERF: Comparer is readonly to allow devirtualization on sealed classes to kick in (JIT 2.0). 
        private readonly TComparer _comparer;
        public TComparer Comparer => _comparer;

        public int Capacity => _capacity;

        public int Count => _size;

        public bool IsEmpty => Count == 0;

        protected FastDictionaryBase(int initialBucketCount, IEnumerable<KeyValuePair<TKey, TValue>> src, TComparer comparer)
            : this(initialBucketCount, comparer)
        {
            Contract.Requires(src != null);
            Contract.Ensures(_capacity >= initialBucketCount);

            foreach (var item in src)
                this[item.Key] = item.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected FastDictionaryBase(FastDictionaryBase<TKey, TValue, TComparer> src, TComparer comparer)
            : this(src._capacity, src, comparer)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected FastDictionaryBase(FastDictionaryBase<TKey, TValue, TComparer> src)
            : this(src._capacity, src, src._comparer)
        { }

        protected FastDictionaryBase(int initialBucketCount, FastDictionaryBase<TKey, TValue, TComparer> src, TComparer comparer)
        {
            Contract.Requires(src != null);
            Contract.Requires(comparer != null);
            Contract.Ensures(_capacity >= initialBucketCount);
            Contract.Ensures(_capacity >= src._capacity);            

            this._comparer = comparer;

            this._initialCapacity = Bits.NextPowerOf2(initialBucketCount);
            this._capacity = Math.Max(src._capacity, initialBucketCount);
            this._capacityMask = this._capacity - 1;
            this._size = src._size;
            this._numberOfUsed = src._numberOfUsed;
            this._numberOfDeleted = src._numberOfDeleted;
            this._nextGrowthThreshold = src._nextGrowthThreshold;

            if (comparer.Equals(src._comparer))
            {
                // Initialization through copy (very efficient) because the comparer is the same.
                this._entries = new Entry[_capacity];
                Array.Copy(src._entries, _entries, _capacity);
            }
            else
            {
                // Initialization through rehashing because the comparer is not the same.
                var entries = new Entry[_capacity];
                BlockCopyMemoryHelper.Memset(entries, new Entry(KUnusedHash, default(TKey), default(TValue)));

                // Creating a temporary alias to use for rehashing.
                this._entries = src._entries;

                // This call will rewrite the aliases
                Rehash(entries);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected FastDictionaryBase(TComparer comparer)
            : this(DictionaryHelper.KInitialCapacity, comparer)
        { }

        protected FastDictionaryBase(int initialBucketCount, TComparer comparer)
        {
            Contract.Requires(comparer != null);
            Contract.Ensures(_capacity >= initialBucketCount);

            this._comparer = comparer;

            // Calculate the next power of 2.
            int newCapacity = initialBucketCount >= DictionaryHelper.KMinBuckets ? initialBucketCount : DictionaryHelper.KMinBuckets;
            newCapacity = Bits.NextPowerOf2(newCapacity);

            this._initialCapacity = newCapacity;

            // Initialization
            this._entries = new Entry[newCapacity];
            BlockCopyMemoryHelper.Memset(this._entries, new Entry(KUnusedHash, default(TKey), default(TValue)));

            this._capacity = newCapacity;
            this._capacityMask = this._capacity - 1;

            this._numberOfUsed = 0;
            this._numberOfDeleted = 0;
            this._size = 0;

            this._nextGrowthThreshold = _capacity * 4 / KLoadFactor;
        }

        public void Add(TKey key, TValue value)
        {
            Contract.Requires(key != null);
            Contract.Ensures(this._numberOfUsed <= this._capacity);

            ResizeIfNeeded();

            int hash = GetInternalHashCode(key); // PERF: This goes first because it can consume lots of registers.
            uint uhash = (uint)hash;

            var entries = _entries;

            int numProbes = 1;
            int capacity = _capacity;
            int bucket = hash & _capacityMask;

            Loop:
            do
            {
                uint nHash = entries[bucket].Hash;
                if (nHash == KUnusedHash)
                {
                    _numberOfUsed++;
                    _size++;
                    goto Set;
                }
                if (nHash == KDeletedHash)
                {
                    _numberOfDeleted--;
                    _size++;
                    goto Set;
                }
                    
                if (nHash == uhash)
                    goto PartialHit;

                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;

#if DEBUG
                if ( numProbes >= 100 )
                    throw new InvalidOperationException("The hash function used for this object is not good enough. The distribution is causing clusters and causing huge slowdowns.");
#else
                if (numProbes >= capacity)
                    break;
#endif
            }
            while (true);

            // PERF: If it happens, it should be rare therefore outside of the critical path. 
            PartialHit:
            if (!_comparer.Equals(entries[bucket].Key, key))
            {
                // PERF: This can happen with a very^3 low probability (assuming your hash function is good enough)
                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;
                goto Loop;
            }
            ThrowWhenDuplicatedKey(key); // We throw here. 

            Set:

            this._entries[bucket].Hash = uhash;
            this._entries[bucket].Key = key;
            this._entries[bucket].Value = value;
        }

        private void ThrowWhenKeyIsNull(TKey key)
        {
            throw new ArgumentNullException(nameof(key));
        }

        private void ThrowWhenDuplicatedKey(TKey key)
        {
            throw new ArgumentException("Cannot add duplicated key.", nameof(key));
        }

        public bool Remove(TKey key)
        {
            Contract.Ensures(this._numberOfUsed < this._capacity);

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            int bucket = Lookup(key);
            if (bucket == InvalidNodePosition)
                return false;

            SetDeleted(bucket);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetDeleted(int node)
        {
            Contract.Ensures(_size <= Contract.OldValue<int>(_size));

            if (_entries[node].Hash < KDeletedHash)
            {
                _entries[node].Hash = KDeletedHash;
                _entries[node].Key = default(TKey);
                _entries[node].Value = default(TValue);

                _numberOfDeleted++;
                _size--;
            }

            Contract.Assert(_numberOfDeleted >= Contract.OldValue<int>(_numberOfDeleted));
            Contract.Assert(_entries[node].Hash == KDeletedHash);

            if (3 * this._numberOfDeleted / 2 > this._capacity - this._numberOfUsed)
            {
                // We will force a rehash with the growth factor based on the current size.
                Shrink(Math.Max(_initialCapacity, _size * 2));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ResizeIfNeeded()
        {
            if (_size >= _nextGrowthThreshold)
            {
                Grow(_capacity * 2);
            }
        }

        private void Shrink(int newCapacity)
        {
            Contract.Requires(newCapacity > _size);
            Contract.Ensures(this._numberOfUsed < this._capacity);

            // Calculate the next power of 2.
            newCapacity = Math.Max(Bits.NextPowerOf2(newCapacity), _initialCapacity);

            var entries = new Entry[newCapacity];
            BlockCopyMemoryHelper.Memset(entries, new Entry(KUnusedHash, default(TKey), default(TValue)));

            Rehash(entries);
        }

        public TValue this[TKey key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Contract.Requires(key != null);
                Contract.Ensures(this._numberOfUsed <= this._capacity);

                int hash = GetInternalHashCode(key); // PERF: This goes first because it can consume lots of registers.

                var entries = _entries;

                int numProbes = 1;
                int capacity = _capacity;
                int bucket = hash & _capacityMask;

                Loop:
                do
                {
                    uint nHash = entries[bucket].Hash;
                    if (nHash == KUnusedHash)
                        goto ReturnFalse;
                    if (nHash == hash)
                        goto PartialHit;

                    bucket = (bucket + numProbes) & _capacityMask;
                    numProbes++;

#if DEBUG
                if ( numProbes >= 100 )
                    throw new InvalidOperationException("The hash function used for this object is not good enough. The distribution is causing clusters and causing huge slowdowns.");
#else
                    if (numProbes >= capacity)
                        break;
#endif
                }
                while (true);

                ReturnFalse:
                return ThrowWhenKeyNotFound();

                PartialHit:
                if (!_comparer.Equals(entries[bucket].Key, key))
                {
                    // PERF: This can happen with a very^3 low probability (assuming your hash function is good enough)
                    bucket = (bucket + numProbes) & _capacityMask;
                    numProbes++;
                    goto Loop;
                }

                return entries[bucket].Value;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Contract.Requires(key != null);
                Contract.Ensures(this._numberOfUsed <= this._capacity);

                ResizeIfNeeded();

                int hash = GetInternalHashCode(key); // PERF: This goes first because it can consume lots of registers.
                uint uhash = (uint)hash;

                var entries = _entries;

                int numProbes = 1;
                int capacity = _capacity;
                int bucket = hash & _capacityMask;

                Loop:
                do
                {
                    uint nHash = entries[bucket].Hash;
                    if (nHash == KUnusedHash)
                    {
                        _numberOfUsed++;
                        _size++;
                        goto Set;
                    }
                    if (nHash == KDeletedHash)
                    {
                        _numberOfDeleted--;
                        _size++;
                        goto Set;
                    }

                    if (nHash == uhash)
                        goto PartialHit;

                    bucket = (bucket + numProbes) & _capacityMask;
                    numProbes++;

#if DEBUG
                if ( numProbes >= 100 )
                    throw new InvalidOperationException("The hash function used for this object is not good enough. The distribution is causing clusters and causing huge slowdowns.");
#else
                    if (numProbes >= capacity)
                        break;
#endif
                }
                while (true);

                // PERF: If it happens, it should be rare therefore outside of the critical path. 
                PartialHit:
                if (!_comparer.Equals(entries[bucket].Key, key))
                {
                    // PERF: This can happen with a very^3 low probability (assuming your hash function is good enough)
                    bucket = (bucket + numProbes) & _capacityMask;
                    numProbes++;
                    goto Loop;
                }

                Set:

                this._entries[bucket].Hash = uhash;
                this._entries[bucket].Key = key;
                this._entries[bucket].Value = value;
            }
        }

        private static TValue ThrowWhenKeyNotFound()
        {
            throw new KeyNotFoundException();
        }

        public void Clear()
        {
            BlockCopyMemoryHelper.Memset(this._entries, new Entry(KUnusedHash, default(TKey), default(TValue)));

            this._numberOfUsed = 0;
            this._numberOfDeleted = 0;
            this._size = 0;
        }

        public bool Contains(TKey key)
        {
            Contract.Ensures(this._numberOfUsed <= this._capacity);

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return (Lookup(key) != InvalidNodePosition);
        }

        private void Grow(int newCapacity)
        {
            Contract.Requires(newCapacity >= _capacity);
            Contract.Ensures((_capacity & (_capacity - 1)) == 0);

            var entries = new Entry[newCapacity];
            BlockCopyMemoryHelper.Memset(entries, new Entry(KUnusedHash, default(TKey), default(TValue)));

            Rehash(entries);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]        
        public bool TryGetValue(TKey key, out TValue value)
        {
            Contract.Requires(key != null);
            Contract.Ensures(this._numberOfUsed <= this._capacity);

            int hash = GetInternalHashCode(key); // PERF: This goes first because it can consume lots of registers.

            var entries = _entries;

            int numProbes = 1;
            int capacity = _capacity;
            int bucket = hash & _capacityMask;

            Loop:
            do
            {
                uint nHash = entries[bucket].Hash;
                if (nHash == KUnusedHash)
                    goto ReturnFalse;
                if (nHash == hash)
                    goto PartialHit;

                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;

#if DEBUG       
                if ( numProbes >= 100 )
                    throw new InvalidOperationException("The hash function used for this object is not good enough. The distribution is causing clusters and causing huge slowdowns.");
#else
                if (numProbes >= capacity)
                    break;
#endif                
            }
            while (true);

            ReturnFalse:
            value = default(TValue);
            return false;

            PartialHit:
            if (!_comparer.Equals(entries[bucket].Key, key))
            {
                // PERF: This can happen with a very^3 low probability (assuming your hash function is good enough)
                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;
                goto Loop;
            }

            value = entries[bucket].Value;
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns>Position of the node in the array</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Lookup(TKey key)
        {
            int hash = GetInternalHashCode(key); // PERF: This goes first because it can consume lots of registers.

            var entries = _entries;

            int numProbes = 1;
            int capacity = _capacity;
            int bucket = hash & _capacityMask;

            Loop:
            do
            {
                uint nHash = entries[bucket].Hash;
                if (nHash == KUnusedHash)
                    goto ReturnFalse;
                if (nHash == hash)
                    goto PartialHit;

                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;

#if DEBUG       
                if ( numProbes >= 100 )
                    throw new InvalidOperationException("The hash function used for this object is not good enough. The distribution is causing clusters and causing huge slowdowns.");
#else
                if (numProbes >= capacity)
                    break;
#endif                
            }
            while (true);

            ReturnFalse: return InvalidNodePosition;

            PartialHit:
            if (!_comparer.Equals(entries[bucket].Key, key))
            {
                // PERF: This can happen with a very^3 low probability (assuming your hash function is good enough)
                bucket = (bucket + numProbes) & _capacityMask;
                numProbes++;
                goto Loop;
            }

            return bucket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetInternalHashCode(TKey key)
        {
            return _comparer.GetHashCode(key) & 0x7FFFFFFF;
        }

        private void Rehash(Entry[] entries)
        {
            var size = 0;

            int newCapacityMask = entries.Length - 1;
            for (int it = 0; it < _entries.Length; it++)
            {
                uint hash = _entries[it].Hash;
                if (hash >= KDeletedHash) // No interest for the process of rehashing, we are skipping it.
                    continue;

                int bucket = (int)hash & newCapacityMask;

                int numProbes = 0;
                while (entries[bucket].Hash != KUnusedHash)
                {
                    numProbes++;
                    bucket = (bucket + numProbes) & newCapacityMask;
                }

                entries[bucket].Hash = hash;
                entries[bucket].Key = _entries[it].Key;
                entries[bucket].Value = _entries[it].Value;

                size++;
            }

            this._capacity = entries.Length;
            this._capacityMask = newCapacityMask;
            this._size = size;
            this._entries = entries;

            this._numberOfUsed = size;
            this._numberOfDeleted = 0;

            this._nextGrowthThreshold = _capacity * 4 / KLoadFactor;
        }


        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array), "The array cannot be null");

            if (array.Rank != 1)
                throw new ArgumentException("Multiple dimensions array are not supporter", nameof(array));

            if (index < 0 || index > array.Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (array.Length - index < Count)
                throw new ArgumentException("The array plus the offset is too small.");

            int count = _capacity;

            var entries = _entries;

            for (int i = 0; i < count; i++)
            {
                if (entries[i].Hash < KDeletedHash)
                    array[index++] = new KeyValuePair<TKey, TValue>(entries[i].Key, entries[i].Value);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Enumerator(this);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new Enumerator(this);
        }


        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private readonly FastDictionaryBase<TKey, TValue, TComparer> _dictionary;
            private int _index;
            private KeyValuePair<TKey, TValue> _current;

            internal const int DictEntry = 1;
            internal const int KeyValuePair = 2;

            internal Enumerator(FastDictionaryBase<TKey, TValue, TComparer> dictionary)
            {
                this._dictionary = dictionary;
                this._index = 0;
                this._current = new KeyValuePair<TKey, TValue>();
            }

            public bool MoveNext()
            {
                var count = _dictionary._capacity;
                var entries = _dictionary._entries;

                // Use unsigned comparison since we set index to dictionary.count+1 when the enumeration ends.
                // dictionary.count+1 could be negative if dictionary.count is Int32.MaxValue
                while (_index < count)
                {
                    if (entries[_index].Hash < KDeletedHash)
                    {
                        _current = new KeyValuePair<TKey, TValue>(entries[_index].Key, entries[_index].Value);
                        _index++;
                        return true;
                    }
                    _index++;
                }

                _index = count + 1;
                _current = new KeyValuePair<TKey, TValue>();
                return false;
            }

            public KeyValuePair<TKey, TValue> Current => _current;

            public void Dispose()
            {
            }

            object IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._capacity + 1))
                        throw new InvalidOperationException("Can't happen.");

                    return new KeyValuePair<TKey, TValue>(_current.Key, _current.Value);
                }
            }

            void IEnumerator.Reset()
            {
                _index = 0;
                _current = new KeyValuePair<TKey, TValue>();
            }
        }

        public KeyCollection Keys => new KeyCollection(this);

        public ValueCollection Values => new ValueCollection(this);

        public bool ContainsKey(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return (Lookup(key) != InvalidNodePosition);
        }

        public bool ContainsValue(TValue value)
        {
            var entries = _entries;
            int count = _capacity;

            if (value == null)
            {
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].Hash < KDeletedHash && entries[i].Value == null)
                        return true;
                }
            }
            else
            {
                EqualityComparer<TValue> c = EqualityComparer<TValue>.Default;
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].Hash < KDeletedHash && c.Equals(entries[i].Value, value))
                        return true;
                }
            }
            return false;
        }


        public sealed class KeyCollection : IEnumerable<TKey>
        {
            private readonly FastDictionaryBase<TKey, TValue, TComparer> _dictionary;

            public KeyCollection(FastDictionaryBase<TKey, TValue, TComparer> dictionary)
            {
                Contract.Requires(dictionary != null);

                this._dictionary = dictionary;
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            public void CopyTo(TKey[] array, int index)
            {
                if (array == null)
                    throw new ArgumentNullException("The array cannot be null", "array");

                if (index < 0 || index > array.Length)
                    throw new ArgumentOutOfRangeException("index");

                if (array.Length - index < _dictionary.Count)
                    throw new ArgumentException("The array plus the offset is too small.");

                int count = _dictionary._capacity;
                var entries = _dictionary._entries;

                for (int i = 0; i < count; i++)
                {
                    if (entries[i].Hash < KDeletedHash)
                        array[index++] = entries[i].Key;
                }
            }

            public int Count => _dictionary.Count;


            IEnumerator<TKey> IEnumerable<TKey>.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }


            public struct Enumerator : IEnumerator<TKey>
            {
                private readonly FastDictionaryBase<TKey, TValue, TComparer> _dictionary;
                private int _index;
                private TKey _currentKey;

                internal Enumerator(FastDictionaryBase<TKey, TValue, TComparer> dictionary)
                {
                    this._dictionary = dictionary;
                    _index = 0;
                    _currentKey = default(TKey);
                }

                public void Dispose()
                {
                }

                public bool MoveNext()
                {
                    var count = _dictionary._capacity;

                    var entries = _dictionary._entries;
                    while (_index < count)
                    {
                        if (entries[_index].Hash < KDeletedHash)
                        {
                            _currentKey = entries[_index].Key;
                            _index++;
                            return true;
                        }
                        _index++;
                    }

                    _index = count + 1;
                    _currentKey = default(TKey);
                    return false;
                }

                public TKey Current => _currentKey;

                Object System.Collections.IEnumerator.Current
                {
                    get
                    {
                        if (_index == 0 || (_index == _dictionary.Count + 1))
                            throw new InvalidOperationException("Cant happen.");

                        return _currentKey;
                    }
                }

                void System.Collections.IEnumerator.Reset()
                {
                    _index = 0;
                    _currentKey = default(TKey);
                }
            }
        }



        public sealed class ValueCollection : IEnumerable<TValue>
        {
            private readonly FastDictionaryBase<TKey, TValue, TComparer> _dictionary;

            public ValueCollection(FastDictionaryBase<TKey, TValue, TComparer> dictionary)
            {
                Contract.Requires(dictionary != null);

                this._dictionary = dictionary;
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            public void CopyTo(TValue[] array, int index)
            {
                if (array == null)
                    throw new ArgumentNullException(nameof(array), "The array cannot be null");

                if (index < 0 || index > array.Length)
                    throw new ArgumentOutOfRangeException(nameof(index));

                if (array.Length - index < _dictionary.Count)
                    throw new ArgumentException("The array plus the offset is too small.");

                int count = _dictionary._capacity;

                var entries = _dictionary._entries;
                for (int i = 0; i < count; i++)
                {
                    if (entries[i].Hash < KDeletedHash)
                        array[index++] = entries[i].Value;
                }
            }

            public int Count => _dictionary.Count;

            IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new Enumerator(_dictionary);
            }


            public struct Enumerator : IEnumerator<TValue>
            {
                private readonly FastDictionaryBase<TKey, TValue, TComparer> _dictionary;
                private int _index;
                private TValue _currentValue;

                internal Enumerator(FastDictionaryBase<TKey, TValue, TComparer> dictionary)
                {
                    _dictionary = dictionary;
                    _index = 0;
                    _currentValue = default(TValue);
                }

                public void Dispose()
                {
                }

                public bool MoveNext()
                {
                    var count = _dictionary._capacity;

                    var entries = _dictionary._entries;
                    while (_index < count)
                    {
                        if (entries[_index].Hash < KDeletedHash)
                        {
                            _currentValue = entries[_index].Value;
                            _index++;
                            return true;
                        }
                        _index++;
                    }

                    _index = count + 1;
                    _currentValue = default(TValue);
                    return false;
                }

                public TValue Current => _currentValue;

                Object IEnumerator.Current
                {
                    get
                    {
                        if (_index == 0 || (_index == _dictionary.Count + 1))
                            throw new InvalidOperationException("Cant happen.");

                        return _currentValue;
                    }
                }

                void IEnumerator.Reset()
                {
                    _index = 0;
                    _currentValue = default(TValue);
                }
            }
        }

        private static class BlockCopyMemoryHelper
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void Memset(Entry[] array, Entry value)
            {
                int block = 64, index = 0;
                int length = Math.Min(block, array.Length);

                //Fill the initial array
                while (index < length)
                {
                    array[index++] = value;
                }

                length = array.Length;
                while (index < length)
                {
                    Array.Copy(array, 0, array, index, Math.Min(block, (length - index)));
                    index += block;

                    block *= 2;
                }
            }
        }
    }
}
