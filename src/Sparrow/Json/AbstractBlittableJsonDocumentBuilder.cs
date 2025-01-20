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

        protected struct BuildingState()
        {
            public ContinuationState State;
            public int MaxPropertyId;
            public CachedProperties.PropertyName CurrentProperty;
            public FastList<PropertyTag> Properties;
            public FastList<BlittableJsonToken> Types;
            public FastList<int> Positions;
            public long FirstWrite;

            internal bool PartialRead;

            public BuildingState(ContinuationState state) : this()
            {
                State = state;
            }
            public BuildingState(ContinuationState state, bool partialRead = false) : this()
            {
                State = state;
                PartialRead = partialRead;
            }

            public BuildingState(ContinuationState state, FastList<BlittableJsonToken> types, FastList<int> positions) : this()
            {
                State = state;
                Types = types;
                Positions = positions;
            }

            public BuildingState(ContinuationState state, FastList<PropertyTag> properties = null, long firstWrite = -1) : this()
            {
                State = state;
                Properties = properties;
                FirstWrite = firstWrite;
            }
        }

        // PERF: The numbers for continuation states have been changed to improve the chance of the
        // JIT to emit a jump-table instead of complex switch.
        protected enum ContinuationState 
        {
            // PERF: Code size optimizations for read method.
            None = 0,

            ReadValue = 1,  
            ReadObjectDocument = 2, 
            ReadArrayDocument = 3,  
            ReadObject = 4, 
            ReadPropertyName = 5, 
            ReadPropertyValue = 6,  
            CompleteDocumentArray = 7,  
            CompleteReadingPropertyValue = 8,  
            ReadArray = 9, 
            ReadArrayValue = 10, 
            CompleteArray = 11,
            CompleteArrayValue = 12, 

            // Support for vector type.
            ReadBufferedArrayValue = 13, 
            ReadBufferedValue = 14, 
            CompleteBufferedArray = 15, 
            CompleteBufferedArrayValue = 16, 
        }

        public struct PropertyTag(byte type, CachedProperties.PropertyName property, int position)
        {
            public int Position = position;
            public CachedProperties.PropertyName Property = property;
            public byte Type = type;

            public PropertyTag(CachedProperties.PropertyName property) : this(0, property, 0) {}
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
            public readonly ListCache<PropertyTag> PropertyCache = new();
            public readonly ListCache<int> PositionsCache = new();
            public readonly ListCache<BlittableJsonToken> TokensCache = new();
        }
    }
}
