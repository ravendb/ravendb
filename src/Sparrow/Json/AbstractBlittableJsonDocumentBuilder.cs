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
            // PERF: Utilizing PerCoreContainer to manage GlobalPoolItem instances.
            // This reduces contention and improves cache locality across multiple cores.
            // On 32-bit platforms, a smaller pool size is used to conserve memory resources.
            GlobalCache = PlatformDetails.Is32Bits 
                ? new PerCoreContainer<GlobalPoolItem>(4) 
                : new PerCoreContainer<GlobalPoolItem>();
        }

        protected AbstractBlittableJsonDocumentBuilder()
        {
            // PERF: Efficiently pulling a GlobalPoolItem from the global cache.
            // If the cache is empty, a new GlobalPoolItem is instantiated.
            // This leverages object pooling to minimize memory allocations and enhance performance.
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

            // PERF: Efficiently clearing the continuation stack by using WeakClear,
            // which avoids removing references since BuildingState is a struct.
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

            // PERF: Added multiple constructors to allow efficient initialization
            // based on different use cases. This reduces the overhead of setting properties
            // individually and enables better inlining by the JIT compiler.
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

        // PERF: Simplified the ContinuationState enum to use sequential integer values.
        // This allows the JIT compiler to emit efficient jump tables for switch statements,
        // reducing branch prediction overhead and improving performance in tight loops.
        protected enum ContinuationState 
        {
            ReadValue = 0,  
            ReadObjectDocument, 
            ReadArrayDocument,  
            ReadObject, 
            ReadPropertyName, 
            ReadPropertyValue,  
            CompleteDocumentArray,  
            CompleteReadingPropertyValue,  
            ReadArray, 
            ReadArrayValue, 
            CompleteArray,
            CompleteArrayValue, 

            // Support for vector type.
            ReadBufferedArrayValue, 
            CompleteBufferedArray, 
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

            private readonly FastList<FastList<T>> _cache = new();

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
