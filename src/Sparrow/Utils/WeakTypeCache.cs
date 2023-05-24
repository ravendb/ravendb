using System;
using System.Buffers;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Utils
{
    public class WeakTypeCache<T> : IDisposable
    {
        private struct CacheItem
        {
            public Type Type;
            public T Value;
        }

        private readonly int _andMask;
        private readonly int _cacheSize;
        private CacheItem[] _cache;

        public WeakTypeCache(int cacheSize = 8)
        {
            if (!Bits.IsPowerOfTwo(cacheSize))
                cacheSize = Bits.PowerOf2(cacheSize);

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _cache = ArrayPool<CacheItem>.Shared.Rent(cacheSize);
            _cacheSize = cacheSize;
        }

        public void Renew()
        {
            ArrayPool<CacheItem>.Shared.Return(_cache, true);
            _cache = ArrayPool<CacheItem>.Shared.Rent(_cacheSize);
        }

        public bool TryGet(Type t, out T value)
        {
            int hash = t.GetHashCode();
            var position = hash & _andMask;

            // We are effectively synchronizing the access to the cache because we cannot copy the whole 
            // node to the stack in a single atomic operation. 
            CacheItem[] cache;
            do
            {
                // Because we are assigning null, noone else can continue from here. 
                cache = Interlocked.Exchange(ref _cache, null);
            }
            while (cache == null);

            var node = cache[position];

            // We return the value after we copy the node to the stack. 
            Interlocked.Exchange(ref _cache, cache);

            if (node.Type == t)
            {
                value = node.Value;
                return true;
            }

            value = default;
            return false;
        }

        public void Put(Type t, T value)
        {
            int hash = t.GetHashCode();
            var position = hash & _andMask;

            // We are effectively synchronizing the access to the cache because we cannot copy the whole 
            // node to the stack in a single atomic operation. 
            CacheItem[] cache;
            do
            {
                // Because we are assigning null, noone else can continue from here. 
                cache = Interlocked.Exchange(ref _cache, null);
            }
            while (cache == null);


            // After here the cache reference will be null, effectively working as a barrier for any other
            // attempt to set values here. 

            // We update the copy with the new value.
            ref var node = ref cache[position];
            node.Type = t;
            node.Value = value;

            // We return the backing array after we updated the values. 
            Interlocked.Exchange(ref _cache, cache);
        }

        public void Dispose()
        {
            ArrayPool<CacheItem>.Shared.Return(_cache, true);
            _cache = null;
        }
    }
}
