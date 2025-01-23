using System;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Collections;

namespace Sparrow.Utils
{
    internal sealed class TypeCache<T>
    {
        private readonly FastList<Tuple<Type, T>>[] _buckets;
        private readonly int _size;

        public TypeCache(int size)
        {
            _buckets = new FastList<Tuple<Type, T>>[size];
            _size = size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(Type type, out T result)
        {
            int bucket = GetBucket(type);

            // We get the data and after that we always work from there to avoid
            // harmful race conditions.
            var storage = _buckets[bucket];
            if (storage == null)
                goto NotFound;

            // The idea is that the type cache is big enough so that type collisions are
            // unlikely occurrences. 
            if (storage.Count != 1)
                return TryGetUnlikely(storage, type, out result);

            ref var item = ref storage.GetAsRef(0);
            if (item.Item1 == type)
            {
                result = item.Item2;
                return true;
            }

            NotFound:
            result = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public bool TryGetUnlikely(FastList<Tuple<Type, T>>  storage, Type type, out T result)
        {
            for (int i = storage.Count - 1; i >= 0; i--)
            {
                ref var item = ref storage.GetAsRef(i);
                if (item.Item1 == type)
                {
                    result = item.Item2;
                    return true;
                }
            }

            result = default;
            return false;
        }

        public void Put(Type type, T value)
        {
            int bucket = GetBucket(type);

            // The idea is that this TypeCache<T> is thread safe. It is better to lose some Put
            // that to allow side effects to happen. The tradeoff is having to recompute in case
            // of race conditions.
            FastList<Tuple<Type,T>> newBucket;
            var storage = _buckets[bucket];
            if (storage == null)
            {
                newBucket = new(4);
            }
            else
            {
                newBucket = new FastList<Tuple<Type, T>>(storage.Count + 1);
                storage.CopyTo(newBucket);
            }

            newBucket.Add(new Tuple<Type, T>(type, value));
            _buckets[bucket] = newBucket;
        }

        private int GetBucket(Type type)
        {
            var hashCode = type.GetHashCode();
            if (hashCode < 0)
                hashCode = -hashCode;

            return hashCode % _size;
        }
    }

    /// <summary>
    /// A lightweight, hash-based cache for associating .NET <see cref="Type"/> objects with values of a generic type <typeparamref name="T"/>.
    /// This cache prioritizes performance over strict synchronization, making it suitable for scenarios where occasional cache misses
    /// are acceptable but incorrect (Type, T) mappings must never occur.
    /// </summary>
    internal sealed class ReplacementTypeCache<T>
    {
        // The backing store of Type -> (Type, T) pairs.
        // We do unsynchronized reads/writes, relying on:
        //  1) Atomic reference assignment in .NET
        //  2) The check (item.Item1 == type) to avoid incorrect Type mismatches
        //  3) It's acceptable for TryGet to return false in race conditions
        private FastList<Tuple<Type, T>> _buckets;
        private readonly int _size;
        private readonly int _mask;

        public ReplacementTypeCache(int size = 64)
        {
            _size = Bits.PowerOf2(size);
            _mask = _size - 1;
            _buckets = new(_size);
        }

        /// <summary>
        /// Attempts to retrieve the cached value for the given Type.
        /// This lookup is thread-safe under races in the sense that
        /// it will never return an incorrect (Type, T) pair. However,
        /// it may return false (i.e., a cache miss) if a concurrent
        /// writer has not yet become visible or if there's a hash collision
        /// for a different Type.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(Type type, out T result)
        {
            // Grab the slot. Note that this read is not synchronized.
            // Under a race, we might see a stale tuple or null => return false.
            var item = _buckets[type.GetHashCode() & _mask];
            if (item is not null && item.Item1 == type)
            {
                result = item.Item2;
                return true;
            }

            // If not matched, return false. 
            // We do not attempt to handle collisions or partial updates.
            Unsafe.SkipInit(out result);
            return false;
        }

        /// <summary>
        /// Puts a (Type, T) pair into the cache at the computed bucket index.
        /// This is unsynchronized and may overwrite an existing entry.
        /// This design means we can lose old entries under collision but 
        /// we never produce a wrong (Type, T) on readers. 
        /// Readers can safely read the tuple reference, but might see it 
        /// late or see an older one => which we accept as a 'miss'.
        /// </summary>
        public void Put(Type type, T value)
        {
            _buckets[type.GetHashCode() & _mask] = new Tuple<Type, T>(type, value);
        }
    }
}
