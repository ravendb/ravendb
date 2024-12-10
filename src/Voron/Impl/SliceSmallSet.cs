using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;

// ReSharper disable StaticMemberInGenericType

namespace Voron.Impl
{
    // The concept of the small set is about a normal dictionary optimized for accessing recently accessed items. 
    // In a sense it behaves like an LRU cache over a dictionary, however, it will not use the backing dictionary unless it has to.
    // It is specially designed to deal with sequence values stored in byte pointers. The main difference with a
    // dictionary is that the small set will calculate the hash ONLY if there are strong candidates that match already. 
    public sealed class SliceSmallSet<TValue> : IDisposable
    {
        private static readonly LockFreeRingBuffer<ArrayPool<SetItem>> PerCoreArrayPools = new(128);
        private readonly ArrayPool<SetItem> _perCorePools;

        private const int Invalid = -1;

        private struct SetItem
        {
            public int Size;
            public ulong Hash;
            public Slice Key;
            public TValue Value;
        }

        private readonly int _length;
        private readonly SetItem[] _items;
        private Dictionary<Slice, TValue> _overflowStorage;
        private int _currentIdx;

        public SliceSmallSet(int size = 0)
        {
            _length = size > Vector<long>.Count ? (size - size % Vector<long>.Count) : Vector<long>.Count;

            if (PerCoreArrayPools.TryDequeue(out _perCorePools) == false)
            {
                _perCorePools = ArrayPool<SetItem>.Create();
            }

            _items = _perCorePools.Rent(_length);

            _overflowStorage = null;
            _currentIdx = Invalid;
        }

        public IEnumerable<TValue> Values => ReturnValues();

        private IEnumerable<TValue> ReturnValues()
        { 
            if (_currentIdx < _length)
            {
                // RavenDB-20947: Since _currentIdx has always the same value of the last valid item, we should iterate until we hit it as the rest
                // of the array may contain stale data belonging to different transactions.
                for (int i = 0; i <= _currentIdx; i++)
                {
                    // RavenDB-20947: This may be the case of the "Cannot add a value in a read only transaction on $Root in Read"
                    // If we don't check for 'HasValue' or that the key size is bigger than zero, we may be returning a removed
                    // value. 
                    ref var item = ref _items[i];
                    if (item.Size != 0)
                        yield return item.Value;
                }
            }
            else
            {
                // Since we had overflow, we cannot trust the LRU to contain the whole valid set. Therefore, we use the backing storage instead.
                foreach (var value in _overflowStorage.Values)
                    yield return value;
            }
        }

        public unsafe void Add(Slice key, TValue value)
        {
            int idx = FindKey(key);
            if (idx != Invalid)
                goto Done;

            // side effect, overflow if needed
            idx = RequestWritableBucket();
            if (idx == Invalid || _currentIdx >= _length)
            {
                Debug.Assert(_overflowStorage != null, "By the time this happens, the backing store must have been already created.");
                _overflowStorage[key] = value;
            }

            Done:
            ref var item = ref _items[idx];
            item.Key = key;
            item.Size = key.Size;
            item.Hash = Hashing.XXHash64.CalculateInline(key.Content.Ptr, (ulong)key.Size);
            item.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int FindKey(Slice key)
        {
            Debug.Assert(key.HasValue, "The key is invalid.");

            if (_currentIdx == Invalid)
                return Invalid;

            int keyLength = key.Size;
            Debug.Assert(keyLength > 0, "The key requested cannot be zero or negative.");

            ulong keyHash = 0;

            byte* keyPtr = key.Content.Ptr;

            // PERF: It may seem strange to increase the size to decrement it as the first
            // loop operation. The reason behind this is to be able to just jump back immediately
            // to the top of the loop as soon as we know the item is not the item we are looking
            // for. 
            int elementIdx = Math.Min(_currentIdx, _length - 1);
            while (elementIdx >= 0)
            {
                var currentIdx = elementIdx;
                elementIdx--;

                ref var item = ref _items[currentIdx];

                // First check, we are not going to look into any string that is not of the correct size
                if (item.Size != keyLength)
                    continue;

                // PERF: Assuming a uniformly random symbol distribution, the chance that first two symbols match
                // (i.e.that S1[1] = S2[1]) is equal to 1/σ, the chance that both first and second symbol pairs
                // match(i.e.that S1[1] = S2[1] and S1[2] = S2[2]) is equal to 1/σ^2, etc. More generally,
                // the probability that there is a match between all characters up to a 1 - indexed position i
                // is equal to 1 / σ^i. We are using that knowledge to quickly get rid of elements.
                ref var candidateKey = ref item.Key;

                Debug.Assert(candidateKey.HasValue, "If there is no way candidate key not have a value since then key size stored would be inconsistent.");

                int midValue = (keyLength - 1) / 2;
                if (key[0] != candidateKey[0] || key[midValue] != candidateKey[midValue] || key[keyLength - 1] != candidateKey[keyLength - 1])
                    continue;

                // If there is a match, we will essentially want to quickly get rid of anything that looks 
                // potentially wrong. For that we will use the hash, which will only get calculated if there
                // are at least 1 strong candidate.
                if (keyHash == 0)
                    keyHash = Hashing.XXHash64.CalculateInline(keyPtr, (ulong)key.Size);

                if (item.Hash != keyHash)
                    continue;

                // We now know that we have an almost sure hit. We will do a final verification at this time.
                if (Memory.CompareInline(keyPtr, candidateKey.Content.Ptr, keyLength) != 0)
                    continue;

                return currentIdx;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int RequestWritableBucket()
        {
            // This will only happen once. When we reach that point, it will trigger and it's done.             
            if (_currentIdx == _length - 1)
            {
                var storage = _overflowStorage ?? new Dictionary<Slice, TValue>(SliceComparer.Instance);
                storage.EnsureCapacity(_currentIdx * 2);

                for (int i = 0; i < _length; i++)
                {
                    ref var item = ref _items[i];

                    // If the key size is 0 then there are no keys in there.
                    if (item.Size == 0)
                        continue;

                    storage[item.Key] = item.Value;
                }

                _overflowStorage = storage;
            }

            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(Slice key, out TValue value)
        {
            int idx = FindKey(key);
            if (idx != Invalid)
            {
                ref var item = ref _items[idx];
                value = item.Value;
                return true;
            }

            if (_currentIdx < _length)
            {
                Unsafe.SkipInit(out value);
                return false;
            }

            return _overflowStorage.TryGetValue(key, out value);
        }

        public void Clear()
        {
            Array.Fill(_items, default);

            _overflowStorage?.Clear();
            _currentIdx = Invalid;
        }

        public void Dispose()
        {
            // If we are holding references, then we will clear the portion of the values array
            // that it is in use.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
            {
                Array.Fill(_items, default);
            }

            _perCorePools.Return(_items);

            PerCoreArrayPools.TryEnqueue(_perCorePools);
        }

        public void Remove(Slice name)
        {
            _overflowStorage?.Remove(name);

            // It can happen that the key is not in the LRU cache, therefore if we cannot find it we are done.
            int idx = FindKey(name);
            if (idx == Invalid)
                return;

            // If we have found it, we are retiring it from the cache.
            _items[idx] = default;
        }
    }
}
