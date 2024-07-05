using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Platform;

namespace Sparrow.Json
{
    public abstract class AbstractBlittableJsonDocumentBuilder : IDisposable
    {
        private bool _disposed;

        private readonly GlobalPoolItem _cacheItem;
        protected readonly ListCache<PropertyTag> _propertiesCache;
        protected readonly ListCache<int> _positionsCache;
        protected readonly ListCache<BlittableJsonToken> _tokensCache;

        private static readonly ObjectPool<FastStack<BuildingState>> ContinuationPool = new (() => new FastStack<BuildingState>(32));

        protected readonly FastStack<BuildingState> _continuationState;

        private static readonly PerCoreContainer<GlobalPoolItem> GlobalCache;

        static AbstractBlittableJsonDocumentBuilder()
        {
            GlobalCache = PlatformDetails.Is32Bits 
                ? new PerCoreContainer<GlobalPoolItem>(4) 
                : new PerCoreContainer<GlobalPoolItem>();
        }

        protected AbstractBlittableJsonDocumentBuilder()
        {
            if (GlobalCache.TryPull(out _cacheItem) == false)
                _cacheItem = new GlobalPoolItem();
            _propertiesCache = _cacheItem.PropertyCache;
            _positionsCache = _cacheItem.PositionsCache;
            _tokensCache = _cacheItem.TokensCache;
            _continuationState = ContinuationPool.Allocate();
        }

        public virtual void Dispose()
        {
            GlobalCache.TryPush(_cacheItem);

            // PERF: We are clearing the array without removing the references because the type is an struct.
            _continuationState.WeakClear();
            ContinuationPool.Free(_continuationState);

            _disposed = true;
        }

        protected void ClearState()
        {
            while (_continuationState.Count > 0)
            {
                var state = _continuationState.Pop();
                _propertiesCache.Return(ref state.Properties);
                _tokensCache.Return(ref state.Types);
                _positionsCache.Return(ref state.Positions);
            }
        }

        [Conditional("DEBUG")]
        protected void AssertNotDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        protected struct BuildingState
        {
            public ContinuationState State;
            public int MaxPropertyId;
            public CachedProperties.PropertyName CurrentProperty;
            public FastList<PropertyTag> Properties;
            public FastList<BlittableJsonToken> Types;
            public FastList<int> Positions;
            public long FirstWrite;
            internal readonly bool PartialRead;

            public BuildingState(ContinuationState state)
            {
                State = state;
            }
            
            public BuildingState(ContinuationState state, int maxPropertyId = 0,
                CachedProperties.PropertyName currentProperty = null,
                FastList<PropertyTag> properties = null, FastList<BlittableJsonToken> types = null, FastList<int> positions = null,
                int firstWrite = 0, bool partialRead = false)
            {
                State = state;
                MaxPropertyId = maxPropertyId;
                CurrentProperty = currentProperty;
                Properties = properties;
                Types = types;
                Positions = positions;
                FirstWrite = firstWrite;
                PartialRead = partialRead;
            }
        }

        protected enum ContinuationState
        {
            ReadPropertyName,
            ReadPropertyValue,
            ReadArray,
            ReadArrayValue,
            ReadObject,
            ReadValue,
            CompleteReadingPropertyValue,
            ReadObjectDocument,
            ReadArrayDocument,
            CompleteDocumentArray,
            CompleteArray,
            CompleteArrayValue
        }

        public readonly struct PropertyTag(byte type = 0, CachedProperties.PropertyName property = null, int position = 0)
        {
            public readonly int Position = position;
            public readonly CachedProperties.PropertyName Property = property;
            public readonly byte Type = type;

            public override string ToString()
            {
                return $"{nameof(Position)}: {Position}, {nameof(Property)}: {Property.Comparer} {Property.PropertyId}, {nameof(Type)}: {(BlittableJsonToken)Type}";
            }
        }

        protected sealed class ListCache<T>
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

        private sealed class GlobalPoolItem
        {
            public readonly ListCache<PropertyTag> PropertyCache = new ListCache<PropertyTag>();
            public readonly ListCache<int> PositionsCache = new ListCache<int>();
            public readonly ListCache<BlittableJsonToken> TokensCache = new ListCache<BlittableJsonToken>();
        }
    }
}
