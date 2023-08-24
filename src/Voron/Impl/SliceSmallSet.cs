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
            if (_overflowStorage == null)
            {
                for (int i = 0; i <= _currentIdx; i++)
                    yield return _values[i];
            }
            else
            {
                foreach (var value in _overflowStorage.Values)
                    yield return value;
            }
        }

        public unsafe void Add(Slice key, TValue value)
        {
            if (_overflowStorage != null)
            {
                goto Overflow;
            }

            int idx = FindKey(key);
            if (idx == Invalid)
            {
                // side affect, overflow if needed
                idx = RequestWritableBucket();
                if (idx == Invalid)
                    goto Overflow;
            }

            _keys[idx] = key;
            _keySizes[idx] = key.Size;
            _keyHashes[idx] = Hashing.XXHash64.CalculateInline(key.Content.Ptr, (ulong)key.Size);
            _values[idx] = value;
            return;
            
            Overflow:
            Debug.Assert(_overflowStorage != null);
            _overflowStorage[key] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int FindKey(Slice key)
        {
            if (_currentIdx == Invalid)
                return Invalid;

            int keyLength = key.Size;
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
                // TODO: Implement a vectorized version of the Equals operation. 
                if (AdvMemory.CompareInline(keyPtr, keys[currentIdx].Content.Ptr, keyLength) != 0)
                    continue;

                return currentIdx;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int RequestWritableBucket()
        {
            if (_overflowStorage != null)
                return Invalid;
            
            if (_currentIdx >= _length - 1)
            {
                var storage = new Dictionary<Slice, TValue>(SliceComparer.Instance);
                storage.EnsureCapacity(_currentIdx * 2);

                for (int i = 0; i <= _currentIdx; i++)
                {
                    if(_keys[i].HasValue == false)
                        continue;
                    storage[_keys[i]] = _values[i];
                }

                _overflowStorage = storage;
                _currentIdx = Invalid;
                return Invalid;
            }

            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(Slice key, out TValue value)
        {
            // PERF: We put this into another method call to shrink the size of TryGetValue in the cases
            // where the inliner would decide to inline the method. Given this method will be rarely executed
            // as if it happens, probably this data structure is not the correct answer; the inliner will 
            // not inline this method ever. 
            [MethodImpl(MethodImplOptions.NoInlining)]
            bool TryGetValueFromOverflowUnlikely(out TValue value)
            {
                return _overflowStorage.TryGetValue(key, out value);
            }

            int idx = FindKey(key);
            if (idx == Invalid)
            {
                if (_overflowStorage == null)
                {
                    Unsafe.SkipInit(out value);
                    return false;
                }

                // If we have overflowed, then we will gonna try to find it there. 
                return TryGetValueFromOverflowUnlikely(out value);
            }

            value = _values[idx];
            return true;
        }

        public void Clear()
        {
            Array.Fill(_keySizes, default);
            Array.Fill(_keyHashes, default);
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
            if (_overflowStorage != null)
            {
                _overflowStorage.Remove(name);
                return;
            }

            int idx = FindKey(name);
            _keys[idx] = default;
            _keySizes[idx] = default;
            _keyHashes[idx] = default;
            _values[idx] = default;
        }
    }
}
