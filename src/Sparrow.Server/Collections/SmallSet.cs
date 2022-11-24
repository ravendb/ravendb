using System;
using System.Buffers;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow.Utils;

namespace Sparrow.Server.Collections
{
    // The concept of the weak small set is about an optimized set that can lose elements when it gets filled.
    // In a sense it behaves like a forgetful LRU cache, but will not track the read accesses. This version of the SmallSet
    // is specially designed to deal with blittable keys in a very efficient manner. Since the scanning of the
    // set is based on SIMD instructions (unless they are not available on the platform) the cost is linear on the size and
    // cost may become too high. For small sets this is much more efficient than a full blown dictionary as a cache,
    // but it can degenerate fast. 
    // IMPORTANT: This implementation is intended to be used where either identity semantics is irrelevant (doesn't matter)
    // or the fallback method to acquire the object if it falls off the cache deals with that accordingly. If you need
    // identity semantics use `SmallSet<TKey, TValue>` instead.
    public sealed class WeakSmallSet<TKey, TValue> : IDisposable 
        where TKey : unmanaged
    {
        private const int Invalid = -1;

        private readonly int _length;
        private readonly TKey[] _keys;
        private readonly TValue[] _values;
        private int _currentIdx;

        public WeakSmallSet(int size = 0)
        {
            _length = size > Vector<TKey>.Count ? (size - size % Vector<TKey>.Count) : Vector<TKey>.Count;
            _keys = ArrayPool<TKey>.Shared.Rent(_length);
            _values = ArrayPool<TValue>.Shared.Rent(_length);
            _currentIdx = -1;
        }

        public void Add(TKey key, TValue value)
        {
            int idx = FindKey(key);
            if (idx == Invalid)
                idx = RequestWritableBucket();

            _keys[idx] = key;
            _values[idx] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FindKey(TKey key)
        {
            int elementIdx = Math.Min(_currentIdx, _length - 1);

            var keys = _keys;
            if (Vector.IsHardwareAccelerated && elementIdx > Vector256<TKey>.Count)
            {
                Vector<TKey> chunk;
                var keyVector = new Vector<TKey>(key);
                while (elementIdx >= Vector256<TKey>.Count)
                {
                    // We subtract because we are going to use that even in the case when there are differences.
                    elementIdx -= Vector256<TKey>.Count;
                    chunk = new Vector<TKey>(keys, elementIdx + 1);
                    chunk = Vector.Equals(keyVector, chunk);
                    if (chunk == Vector<TKey>.Zero)
                        continue;

                    elementIdx = Math.Min(elementIdx + Vector256<TKey>.Count, _length - 1);
                    goto Found; 
                }

                chunk = new Vector<TKey>(keys);
                chunk = Vector.Equals(keyVector, chunk);
                if (chunk == Vector<TKey>.Zero)
                    return Invalid;

                elementIdx = Vector256<TKey>.Count - 1;

                Found:
                elementIdx = Math.Min(elementIdx, _currentIdx);
            }

            while (elementIdx >= 0)
            {
                ref TKey current = ref keys[elementIdx];
                if (current.Equals(key))
                    return elementIdx;

                elementIdx--;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int RequestWritableBucket()
        {
            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            int idx = FindKey(key);
            if (idx == Invalid)
            {
                Unsafe.SkipInit(out value);
                return false;
            }
                
            value = _values[idx];
            return true;
        }

        public void Clear()
        {
            _currentIdx = -1;
        }

        public void Dispose()
        {
            ArrayPool<TKey>.Shared.Return(_keys);
            ArrayPool<TValue>.Shared.Return(_values);
        }
    }

    // The concept of the small set is about a normal dictionary optimized for accessing recently accessed items. 
    // In a sense it behaves like a LRU cache over a dictionary, however, it will not use the backing dictionary unless it has to.
    // This version of the Small Set is specially designed to deal with blittable keys in a very efficient manner.
    // Since the scanning of the set is based on SIMD instructions (unless they are not available on the platform)
    // the cost is linear on the size and may become too high if there are too many novel accesses.
    public sealed class SmallSet<TKey, TValue> : IDisposable
        where TKey : unmanaged
    {
        private const int Invalid = -1;

        private readonly int _length;
        private readonly TKey[] _keys;
        private readonly TValue[] _values;
        private Dictionary<TKey, TValue> _overflowStorage;
        private int _currentIdx;

        public SmallSet(int size = 0)
        {
            _length = size > Vector<TKey>.Count ? (size - size % Vector<TKey>.Count) : Vector<TKey>.Count;
            _keys = ArrayPool<TKey>.Shared.Rent(_length);
            _values = ArrayPool<TValue>.Shared.Rent(_length);
            _overflowStorage = null;
            _currentIdx = -1;
        }


        public void Add(TKey key, TValue value)
        {
            int idx = FindKey(key);
            if (idx == Invalid)
                idx = RequestWritableBucket();

            // We have overflowed already. 
            if (_overflowStorage != null)
                _overflowStorage[key] = value;

            _keys[idx] = key;
            _values[idx] = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FindKey(TKey key)
        {
            int elementIdx = Math.Min(_currentIdx, _length - 1);

            var keys = _keys;
            if (Vector.IsHardwareAccelerated && elementIdx > Vector256<TKey>.Count)
            {
                Vector<TKey> chunk;
                var keyVector = new Vector<TKey>(key);
                while (elementIdx >= Vector256<TKey>.Count)
                {
                    // We subtract because we are going to use that even in the case when there are differences.
                    elementIdx -= Vector256<TKey>.Count;
                    chunk = new Vector<TKey>(keys, elementIdx + 1);
                    chunk = Vector.Equals(keyVector, chunk);
                    if (chunk == Vector<TKey>.Zero)
                        continue;

                    elementIdx = Math.Min(elementIdx + Vector256<TKey>.Count, _length - 1);
                    goto Found;
                }

                chunk = new Vector<TKey>(keys);
                chunk = Vector.Equals(keyVector, chunk);
                if (chunk == Vector<TKey>.Zero)
                    return Invalid;

                elementIdx = Vector256<TKey>.Count - 1;

            Found:
                elementIdx = Math.Min(elementIdx, _currentIdx);
            }

            while (elementIdx >= 0)
            {
                ref TKey current = ref keys[elementIdx];
                if (current.Equals(key))
                    return elementIdx;

                elementIdx--;
            }

            return Invalid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int RequestWritableBucket()
        {
            if (_currentIdx >= _length && _overflowStorage == null)
            {
                var storage = new Dictionary<TKey, TValue>(_currentIdx * 2);
                for (int i = 0; i <= _currentIdx; i++)
                    storage[_keys[i]] = _values[i];
                _overflowStorage = storage;
            }
            
            _currentIdx++;
            return _currentIdx % _length;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            // PERF: We put this into another method call to shrink the size of TryGetValue in the cases
            // where the inliner would decide to inline the method. Given this method will be rarely executed
            // as if it happens, probably this data structure is not the correct answer; the inliner will 
            // not inline this method ever. 
            [MethodImpl(MethodImplOptions.NoInlining)]
            bool TryGetValueFromOverflowUnlikely(out TValue value)
            {
                if (_overflowStorage.TryGetValue(key, out value))
                    return true;

                return false;
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
            Array.Fill(_keys, (TKey)(object)-1);
            Array.Fill(_values, default);
            _overflowStorage?.Clear();
            _currentIdx = -1;
        }

        public void Dispose()
        {
            ArrayPool<TKey>.Shared.Return(_keys);
            ArrayPool<TValue>.Shared.Return(_values);
        }
    }
}
