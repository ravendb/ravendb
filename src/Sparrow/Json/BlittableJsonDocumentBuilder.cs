using System;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Primitives;
using Sparrow.Collections;
using Sparrow.Exceptions;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Sparrow.Json
{
    public sealed class BlittableJsonDocumentBuilder : IDisposable
    {
        private class GlobalPoolItem
        {
            public struct ResetBehavior : IResetSupport<GlobalPoolItem>
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public void Reset(GlobalPoolItem value)
                {
                    value.PropertyCache.Reset();
                    value.PositionsCache.Reset();
                    value.TokensCache.Reset();
                }
            }

            public readonly ListCache<PropertyTag> PropertyCache = new ListCache<PropertyTag>();
            public readonly ListCache<int> PositionsCache = new ListCache<int>();
            public readonly ListCache<BlittableJsonToken> TokensCache = new ListCache<BlittableJsonToken>();

            public void Reset()
            {
                ResetBehavior behavior;
                behavior.Reset(this);
            }
        }

        private static readonly ObjectPool<GlobalPoolItem, GlobalPoolItem.ResetBehavior> GlobalCache = new ObjectPool<GlobalPoolItem,GlobalPoolItem.ResetBehavior>( () => new GlobalPoolItem() );

        private static readonly StringSegment UnderscoreSegment = new StringSegment("_");

        private readonly FastStack<BuildingState> _continuationState = new FastStack<BuildingState>();

        private readonly JsonOperationContext _context;
        private UsageMode _mode;
        private readonly IJsonParser _reader;
        public IBlittableDocumentModifier _modifier;
        private readonly BlittableWriter<UnmanagedWriteBuffer> _writer;
        private readonly JsonParserState _state;
        private LazyStringValue _fakeFieldName;

        private readonly SingleUseFlag _disposed = new SingleUseFlag();

        private WriteToken _writeToken;
        private  string _debugTag;

        private readonly GlobalPoolItem _cacheItem;
        private readonly ListCache<PropertyTag> _propertiesCache;
        private readonly ListCache<int> _positionsCache;
        private readonly ListCache<BlittableJsonToken> _tokensCache;

        private class ListCache<T>
        {
            private readonly FastList<FastList<T>> _cache = new FastList<FastList<T>>();
            private int _index = 0;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public FastList<T> Allocate()
            {
                if (_index != _cache.Count)
                    return _cache[_index++];

                var n = new FastList<T>();
                _cache.Add(n);
                _index++;
                return n;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                for (var i = 0; i < _index; i++)
                {
                    var t = _cache[i];
                    t.Clear();
                }
                _index = 0;
            }
        }


        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, IJsonParser reader,
            BlittableWriter<UnmanagedWriteBuffer> writer = null,
            IBlittableDocumentModifier modifier = null)
        {
            _context = context;
            _state = state;
            _reader = reader;
            _modifier = modifier;
            _writer = writer ?? new BlittableWriter<UnmanagedWriteBuffer>(context);

            _cacheItem = GlobalCache.Allocate();
            _propertiesCache = _cacheItem.PropertyCache;
            _positionsCache = _cacheItem.PositionsCache;
            _tokensCache = _cacheItem.TokensCache;            
        }

        public BlittableJsonDocumentBuilder(
            JsonOperationContext context, 
            UsageMode mode, string debugTag, 
            IJsonParser reader, JsonParserState state, 
            BlittableWriter<UnmanagedWriteBuffer> writer = null,
            IBlittableDocumentModifier modifier = null) : this(context, state, reader, writer, modifier)
        {
            Renew(debugTag, mode);
        }

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, UsageMode mode, string debugTag, IJsonParser reader, BlittableWriter<UnmanagedWriteBuffer> writer = null) : this(context, state, reader, writer)
        {
            Renew(debugTag, mode);
        }

        public void Reset()
        {
            _debugTag = null;
            _mode = UsageMode.None;
            _continuationState.Clear();
            _writeToken = default(WriteToken);
            _writer.Reset();
            _cacheItem.Reset();
        }

        public void Renew(string debugTag, UsageMode mode)
        {
            _writeToken = default(WriteToken);
            _debugTag = debugTag;
            _mode = mode;

            _continuationState.Clear();          
            _cacheItem.Reset();

            _writer.ResetAndRenew();
            _modifier?.Reset(_context);

            _fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);            
        }

        public void ReadArrayDocument()
        {
            _continuationState.Push(new BuildingState(ContinuationState.ReadArrayDocument));
        }

        public void ReadObjectDocument()
        {
            _continuationState.Push(new BuildingState(ContinuationState.ReadObjectDocument));
        }

        public void ReadNestedObject()
        {
            _continuationState.Push(new BuildingState(ContinuationState.ReadObject));
        }

        public int SizeInBytes => _writer.SizeInBytes;


        public void Dispose()
        {
            if (_disposed.Raise() == false)
                return;

            _writer.Dispose();
            GlobalCache.Free(_cacheItem);
        }

        private bool ReadInternal<TWriteStrategy>() where TWriteStrategy : IWriteStrategy
        {
            var continuationState = _continuationState;
            var currentState = continuationState.Pop();            
            var reader = _reader;
            var state = _state;
            while (true)
            {
                switch (currentState.State)
                {
                    case ContinuationState.ReadObjectDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            goto ReturnFalse;
                        }
                        currentState.State = ContinuationState.ReadObject;
                        continue;
                    case ContinuationState.ReadArrayDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            goto ReturnFalse;
                        }

                        var fakeProperty = _context.CachedProperties.GetProperty(_fakeFieldName);
                        currentState.CurrentProperty = fakeProperty;
                        currentState.MaxPropertyId = fakeProperty.PropertyId;
                        currentState.FirstWrite = _writer.Position;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.Properties.Add( new PropertyTag { Property = fakeProperty } );
                        currentState.State = ContinuationState.CompleteDocumentArray;
                        continuationState.Push(currentState);
                        currentState = new BuildingState(ContinuationState.ReadArray);
                        continue;

                    case ContinuationState.CompleteDocumentArray:
                        currentState.Properties[0] = new PropertyTag(                        
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.Properties[0].Property,                           
                            position: _writeToken.ValuePos
                        );

                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                        goto ReturnTrue;

                    case ContinuationState.ReadObject:
                        if (state.CurrentTokenType == JsonParserToken.StartObject)
                        {
                            currentState.State = ContinuationState.ReadPropertyName;
                            currentState.Properties = _propertiesCache.Allocate();
                            currentState.FirstWrite = _writer.Position;
                            continue;
                        }

                        goto ErrorExpectedStartOfObject;

                    case ContinuationState.ReadArray:
                        if (state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                            currentState.Types = _tokensCache.Allocate();
                            currentState.Positions = _positionsCache.Allocate();
                            currentState.State = ContinuationState.ReadArrayValue;
                            continue;
                        }

                        goto ErrorExpectedStartOfArray;                        

                    case ContinuationState.ReadArrayValue:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            goto ReturnFalse;
                        }

                        if (state.CurrentTokenType == JsonParserToken.EndArray)
                        {
                            currentState.State = ContinuationState.CompleteArray;
                            continue;
                        }

                        currentState.State = ContinuationState.CompleteArrayValue;
                        continuationState.Push(currentState);
                        currentState = new BuildingState(ContinuationState.ReadValue);
                        continue;

                    case ContinuationState.CompleteArrayValue:
                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        currentState.State = ContinuationState.ReadArrayValue;
                        continue;

                    case ContinuationState.CompleteArray:
                        var arrayToken = BlittableJsonToken.StartArray;
                        var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types, ref arrayToken);
                        _writeToken = new WriteToken(arrayInfoStart, arrayToken);
                        currentState = continuationState.Pop();
                        continue;

                    case ContinuationState.ReadPropertyName:
                        if (ReadMaybeModifiedPropertyName() == false)
                        {
                            continuationState.Push(currentState);
                            goto ReturnFalse;
                        }

                        if (state.CurrentTokenType == JsonParserToken.EndObject)
                        {
                            _modifier?.EndObject();
                            _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                            if (continuationState.Count == 0)
                                goto ReturnTrue;

                            currentState = continuationState.Pop();
                            continue;
                        }

                        if (state.CurrentTokenType != JsonParserToken.String)
                            goto ErrorExpectedProperty;

                        var property = CreateLazyStringValueFromParserState();
                        currentState.CurrentProperty = _context.CachedProperties.GetProperty(property);
                        currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
                        currentState.State = ContinuationState.ReadPropertyValue;
                        continue;
                    case ContinuationState.ReadPropertyValue:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            goto ReturnFalse;
                        }
                        currentState.State = ContinuationState.CompleteReadingPropertyValue;
                        continuationState.Push(currentState);
                        currentState = new BuildingState(ContinuationState.ReadValue);
                        continue;
                    case ContinuationState.CompleteReadingPropertyValue:
                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        currentState.Properties.Add(new PropertyTag (
                            position: _writeToken.ValuePos,
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.CurrentProperty));

                        currentState.State = ContinuationState.ReadPropertyName;
                        continue;
                    case ContinuationState.ReadValue:
                        ReadJsonValue<TWriteStrategy>();                        
                        currentState = _continuationState.Pop();
                        break;
                }
            }

            ReturnTrue: return true;
            ReturnFalse: return false;

            ErrorExpectedProperty: ThrowExpectedProperty();
            ErrorExpectedStartOfObject: ThrowExpectedStartOfObject();
            ErrorExpectedStartOfArray: ThrowExpectedStartOfArray();
            return false; // Will never execute.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            if (_continuationState.Count == 0)
                return false; //nothing to do

            if (_mode == UsageMode.None)
            {
                return ReadInternal<WriteNone>();
            }

            return ReadInternal<WriteFull>();
        }

        private bool ReadMaybeModifiedPropertyName()
        {
            if (_modifier != null)
            {
                return _modifier.AboutToReadPropertyName(_reader, _state);
            }
            return _reader.Read();
        }

        private void ThrowExpectedProperty()
        {
            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfArray()
        {
            throw new InvalidStartOfObjectException("Expected start of array, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfObject()
        {
            throw new InvalidStartOfObjectException("Expected start of object, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private interface IWriteStrategy { }
        private struct WriteFull : IWriteStrategy { }
        private struct WriteNone : IWriteStrategy { }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void ReadJsonValue<TWriteStrategy>() where TWriteStrategy : IWriteStrategy
        {
            int start;
            JsonParserToken current = _state.CurrentTokenType;
            if (current == JsonParserToken.String)
            {
                BlittableJsonToken stringToken;
                if (typeof(TWriteStrategy) == typeof(WriteNone))
                {
                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, _state.EscapePositions, out stringToken, _mode, _state.CompressedSize);
                }
                else // WriteFull
                {
                    if (_state.EscapePositions.Count == 0 && _state.CompressedSize == null && (_mode & UsageMode.CompressSmallStrings) == 0 && _state.StringSize < 128)
                    {
                        start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);
                        stringToken = BlittableJsonToken.String;
                    }
                    else
                    {
                        start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, _state.EscapePositions, out stringToken, _mode, _state.CompressedSize);
                    }                        
                }
                _state.CompressedSize = null;
                _writeToken = new WriteToken(start, stringToken);
            }
            else if (current == JsonParserToken.Integer)
            {
                start = _writer.WriteValue(_state.Long);
                _writeToken = new WriteToken(start, BlittableJsonToken.Integer);
            }
            else if (current == JsonParserToken.StartObject)
            {
                _modifier?.StartObject();
                _continuationState.Push(new BuildingState(ContinuationState.ReadObject));
            }
            else if (current != JsonParserToken.EndObject)
            { 
                ReadJsonValueUnlikely<TWriteStrategy>(current);
            }       
        }

        private unsafe void ReadJsonValueUnlikely<TWriteStrategy>(JsonParserToken current) where TWriteStrategy : IWriteStrategy
        {
            int start;
            switch (current)
            {
                case JsonParserToken.StartArray:
                    _continuationState.Push(new BuildingState(ContinuationState.ReadArray));
                    return;
                case JsonParserToken.Float:
                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                        _reader.ValidateFloat();

                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);

                    _state.CompressedSize = null;
                    _writeToken = new WriteToken(start, BlittableJsonToken.LazyNumber);
                    return;
                case JsonParserToken.True:
                case JsonParserToken.False:
                    start = _writer.WriteValue(current == JsonParserToken.True ? (byte)1 : (byte)0);
                    _writeToken = new WriteToken(start, BlittableJsonToken.Boolean);
                    return;
                case JsonParserToken.Null:
                    // nothing to do here, we handle that with the token
                    start = _writer.WriteValue((byte)0);
                    _writeToken = new WriteToken(start, BlittableJsonToken.Null);
                    return;
            }

            ThrowExpectedValue(current);
        }

        private void ThrowExpectedValue(JsonParserToken token)
        {
            throw new InvalidDataException("Expected a value, but got " + token);
        }


        public enum ContinuationState
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

        public struct BuildingState
        {
            public ContinuationState State;
            public int MaxPropertyId;
            public CachedProperties.PropertyName CurrentProperty;
            public FastList<PropertyTag> Properties;
            public FastList<BlittableJsonToken> Types;
            public FastList<int> Positions;
            public long FirstWrite;

            public BuildingState(ContinuationState state)
            {
                State = state;
                MaxPropertyId = 0;
                CurrentProperty = null;
                Properties = null;
                Types = null;
                Positions = null;
                FirstWrite = 0;
            }
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

        [Flags]
        public enum UsageMode
        {
            None = 0,
            ValidateDouble = 1,
            CompressStrings = 2,
            CompressSmallStrings = 4,
            ToDisk = ValidateDouble | CompressStrings
        }

        public struct WriteToken
        {
            public int ValuePos;
            public BlittableJsonToken WrittenToken;

            public WriteToken(int position, BlittableJsonToken token)
            {
                ValuePos = position;
                WrittenToken = token;
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe LazyStringValue CreateLazyStringValueFromParserState()
        {
            var lazyStringValueFromParserState = _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize);
            if (_state.EscapePositions.Count <= 0)
                return lazyStringValueFromParserState;

            lazyStringValueFromParserState.EscapePositions = _state.EscapePositions.ToArray();
            return lazyStringValueFromParserState;
        }

        public void FinalizeDocument()
        {
            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);
            _cacheItem.Reset();
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _writer.CreateReader();
        }

        public BlittableJsonReaderArray CreateArrayReader(bool noCache)
        {
            var reader = CreateReader();
            reader.NoCache = noCache;
            if (reader.TryGet("_", out BlittableJsonReaderArray array))
                return array;
            throw new InvalidOperationException("Couldn't find array");
        }

        public override string ToString()
        {
            return "Building json for " + _debugTag;
        }

        public bool NeedResetPropertiesCache()
        {
            return _context.CachedProperties.PropertiesDiscovered > CachedProperties.CachedPropertiesSize;
        }
    }

    public interface IBlittableDocumentModifier
    {
        void StartObject();
        void EndObject();
        bool AboutToReadPropertyName(IJsonParser reader, JsonParserState state);
        void Reset(JsonOperationContext context);
    }
}
