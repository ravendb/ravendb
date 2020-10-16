using System;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Platform;

namespace Sparrow.Json
{
    public abstract class BlittableJsonDocumentBuilderCache : IDisposable
    {
        private readonly GlobalPoolItem _cacheItem;
        protected readonly ListCache<PropertyTag> _propertiesCache;
        protected readonly ListCache<int> _positionsCache;
        protected readonly ListCache<BlittableJsonToken> _tokensCache;

        protected BlittableJsonDocumentBuilderCache()
        {
            if (GlobalCache.TryPull(out _cacheItem) == false)
                _cacheItem = new GlobalPoolItem();
            _propertiesCache = _cacheItem.PropertyCache;
            _positionsCache = _cacheItem.PositionsCache;
            _tokensCache = _cacheItem.TokensCache;
        }

        public virtual void Dispose()
        {
            GlobalCache.Push(_cacheItem);
        }

        public struct PropertyTag
        {
            public int Position;

            public override string ToString()
            {
                return $"{nameof(Position)}: {Position}, {nameof(Property)}: {Property.Comparer} {Property.PropertyId}, {nameof(Type)}: {(BlittableJsonToken)Type}";
            }

            public CachedProperties.PropertyName Property;
            public byte Type;

            public PropertyTag(byte type, CachedProperties.PropertyName property, int position)
            {
                Type = type;
                Property = property;
                Position = position;
            }
        }

        private class GlobalPoolItem
        {
            public readonly ListCache<PropertyTag> PropertyCache = new ListCache<PropertyTag>();
            public readonly ListCache<int> PositionsCache = new ListCache<int>();
            public readonly ListCache<BlittableJsonToken> TokensCache = new ListCache<BlittableJsonToken>();
        }

        private static readonly PerCoreContainer<GlobalPoolItem> GlobalCache = new PerCoreContainer<GlobalPoolItem>();

        protected class ListCache<T>
        {
            private static readonly int MaxSize = PlatformDetails.Is32Bits ? 256 : 1024;

            private readonly FastList<FastList<T>> _cache = new FastList<FastList<T>>();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public FastList<T> Allocate()
            {
                return _cache.RemoveLast() ?? new FastList<T>();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Return(ref FastList<T> n)
            {
                if (n == null)
                    return;
                if (_cache.Count >= MaxSize)
                {
                    n = null;
                    return;
                }
                n.Clear();
                _cache.Add(n);
                n = null;
            }
        }
    }
}
