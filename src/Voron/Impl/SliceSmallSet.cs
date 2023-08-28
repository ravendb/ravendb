using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
// ReSharper disable StaticMemberInGenericType

namespace Voron.Impl
{
    // The concept of the small set is about a normal dictionary optimized for accessing recently accessed items. 
    // In a sense it behaves like a LRU cache over a dictionary, however, it will not use the backing dictionary unless it has to.
    // It is specially designed to deal with sequence values stored in byte pointers. The main difference with a
    // dictionary is that the small set will calculate the hash ONLY if there are strong candidates that match already. 
    public sealed class SliceSmallSet<TValue> : IDisposable
    {
        private static readonly ArrayPool<int> KeySizesPool = ArrayPool<int>.Create();
        private static readonly ArrayPool<ulong> KeyHashesPool = ArrayPool<ulong>.Create();
        private static readonly ArrayPool<Slice> KeysPool = ArrayPool<Slice>.Create();
        private static readonly ArrayPool<TValue> ValuesPool = ArrayPool<TValue>.Create();

        private const int Invalid = -1;

        private readonly int _length;
        private readonly int[] _keySizes;
        private readonly ulong[] _keyHashes;
        private readonly Slice[] _keys;
        private readonly TValue[] _values;
        private Dictionary<Slice, TValue> _overflowStorage;
        private int _currentIdx;

        public SliceSmallSet(int size = 0)
        {
            _length = size > Vector<long>.Count ? (size - size % Vector<long>.Count) : Vector<long>.Count;
            _keySizes = KeySizesPool.Rent(_length);
            _keyHashes = KeyHashesPool.Rent(_length);
            _keys = KeysPool.Rent(_length);
            _values = ValuesPool.Rent(_length);
            _overflowStorage = null;
            _currentIdx = Invalid;
        }

        public IEnumerable<TValue> Values => ReturnValues();

        private IEnumerable<TValue> ReturnValues()
        { 
            if (_currentIdx < _length)
            {
                for (int i = 0; i < _length; i++)
                {
                    // RavenDB-20947: This may be the case of the "Cannot add a value in a read only transaction on $Root in Read"
                    // If we don't check for 'HasValue' or that the key size is bigger than zero, we may be returning a removed
                    // or a tree belonging to a different transaction if the array has not been properly cleaned. 
                    if (_keySizes[i] != 0)
                        yield return _values[i];
                }
            }
            else
            {
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
            _keys[idx] = key;
            _keySizes[idx] = key.Size;
            _keyHashes[idx] = Hashing.XXHash64.CalculateInline(key.Content.Ptr, (ulong)key.Size);
            _values[idx] = value;
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

            Slice[] keys = _keys;
            int[] keySizes = _keySizes;
            ulong[] keyHashes = _keyHashes;
            byte* keyPtr = key.Content.Ptr;

            // PERF: It may seems strange to increase the size to decrement it as the first
            // loop operation. The reason behind this is to be able to just jump back immediately
            // to the top of the loop as soon as we know the item is not the item we are looking
            // for. 
            int elementIdx = Math.Min(_currentIdx, _length - 1);
            while (elementIdx >= 0)
            {
                var currentIdx = elementIdx;
                elementIdx--;

                // First check, we are not going to look into any string that is not of the correct size
                if (keySizes[currentIdx] != keyLength)
                    continue;

                // PERF: Assuming a uniformly random symbol distribution, the chance that first two symbols match
                // (i.e.that S1[1] = S2[1]) is equal to 1/σ, the chance that both first and second symbol pairs
                // match(i.e.that S1[1] = S2[1] and S1[2] = S2[2]) is equal to 1/σ^2, etc. More generally,
                // the probability that there is a match between all characters up to a 1 - indexed position i
                // is equal to 1 / σ^i. We are using that knowledge to quickly get rid of elements.
                ref var candidateKey = ref keys[currentIdx];

                Debug.Assert(candidateKey.HasValue, "If there is no way candidate key not have a value since then key size stored would be inconsistent.");

                int midValue = (keyLength - 1) / 2;
                if (key[0] != candidateKey[0] || key[midValue] != candidateKey[midValue] || key[keyLength - 1] != candidateKey[keyLength - 1])
                    continue;

                // If there is a match, we will essentially want to quickly get rid of anything that looks 
                // potentially wrong. For that we will use the hash, which will only get calculated if there
                // are at least 1 strong candidate.
                if (keyHash == 0)
                    keyHash = Hashing.XXHash64.CalculateInline(keyPtr, (ulong)key.Size);

                if (keyHashes[currentIdx] != keyHash)
                    continue;

                // We now know that we have an almost sure hit. We will do a final verification at this time.
                if (AdvMemory.CompareInline(keyPtr, candidateKey.Content.Ptr, keyLength) != 0)
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
                    // If the key size is 0 then there are no keys in there.
                    if (_keySizes[i] == 0)
                        continue;

                    storage[_keys[i]] = _values[i];
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
                value = _values[idx];
                return true;
            }

            if (_currentIdx < _length)
            {
                Unsafe.SkipInit(out value);
                return false;
            }

            // PERF: We put this into another method call to shrink the size of TryGetValue in the cases
            // where the inliner would decide to inline the method. Given this method will be rarely executed
            // as if it happens, probably this data structure is not the correct answer; the inliner will 
            // not inline this method ever. 
            [MethodImpl(MethodImplOptions.NoInlining)]
            bool TryGetValueFromOverflowUnlikely(out TValue value)
            {
                return _overflowStorage.TryGetValue(key, out value);
            }

            // If we have overflowed, then we will gonna try to find it there. 
            return TryGetValueFromOverflowUnlikely(out value);
        }

        public void Clear()
        {
            Array.Fill(_keySizes, 0);
            Array.Fill(_keyHashes, 0ul);
            Array.Fill(_keys, default);
            Array.Fill(_values, default);
            _overflowStorage?.Clear();
            _currentIdx = Invalid;
        }

        public void Dispose()
        {
            KeySizesPool.Return(_keySizes, clearArray: true);
            KeyHashesPool.Return(_keyHashes, clearArray: true);
            KeysPool.Return(_keys, clearArray: true);
            ValuesPool.Return(_values, clearArray: true);
        }

        public void Remove(Slice name)
        {
            _overflowStorage?.Remove(name);

            // It can happen that the key is not in the LRU cache, therefore if we cannot find it we are done.
            int idx = FindKey(name);
            if (idx == Invalid)
                return;

            // If we have found it, we are retiring it from the cache.
            _keys[idx] = default;
            _keySizes[idx] = 0;
            _keyHashes[idx] = 0ul;
            _values[idx] = default;
        }
    }
}
