using System;
using System.Runtime.CompilerServices;
using Sparrow.Collections;

namespace Sparrow.Utils
{
    internal sealed class TypeCache<T>(int size)
    {
        private readonly FastList<Tuple<Type, T>>[] _buckets = new FastList<Tuple<Type, T>>[size];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(Type type, out T result)
        {
            int typeHash = type.GetHashCode();

            // We get the data and after that we always work from there to avoid
            // harmful race conditions.
            var storage = _buckets[typeHash % size];
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
            int typeHash = type.GetHashCode();
            int bucket = typeHash % size;

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
    }
}
