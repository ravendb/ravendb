using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Exceptions;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Sparrow.Json
{
    public sealed class BlittableJsonDocumentBuilder : AbstractBlittableJsonDocumentBuilder, IDisposableQueryable
    {
        private static readonly StringSegment UnderscoreSegment = new("_");

        private readonly JsonOperationContext _context;
        private UsageMode _mode;
        private readonly IJsonParser _reader;
        public IBlittableDocumentModifier _modifier;
        private readonly BlittableWriter<UnmanagedWriteBuffer> _writer;
        private readonly JsonParserState _state;
        private LazyStringValue _fakeFieldName;

        private readonly SingleUseFlag _disposed = new();

        private WriteToken _writeToken;
        private string _debugTag;

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, IJsonParser reader,
            BlittableWriter<UnmanagedWriteBuffer> writer = null,
            IBlittableDocumentModifier modifier = null)
        {
            _context = context;
            _state = state;
            _reader = reader;
            _modifier = modifier;
            _writer = writer ?? new BlittableWriter<UnmanagedWriteBuffer>(context);
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

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, UsageMode mode, string debugTag, IJsonParser reader,
            BlittableWriter<UnmanagedWriteBuffer> writer = null) : this(context, state, reader, writer)
        {
            Renew(debugTag, mode);
        }

        bool IDisposableQueryable.IsDisposed => _disposed.IsRaised();

        public void Reset()
        {
            AssertNotDisposed();

            _debugTag = null;
            _mode = UsageMode.None;

            ClearState();

            _writeToken = default;
            _writer.Reset();
        }

        public void Renew(string debugTag, UsageMode mode, IBlittableDocumentModifier modifier)
        {
            _modifier = modifier;
            Renew(debugTag, mode);
        }

        public void Renew(string debugTag, UsageMode mode)
        {
            AssertNotDisposed();

            _writeToken = default;
            _debugTag = debugTag;
            _mode = mode;

            ClearState();

            _writer.ResetAndRenew();
            _modifier?.Reset(_context);

            _fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
        }

        public void ReadArrayDocument()
        {
            ref var item = ref _continuationState.PushByRef();
            item = new BuildingState(ContinuationState.ReadArrayDocument);
        }

        public void ReadObjectDocument()
        {
            ref var item = ref _continuationState.PushByRef();
            item = new BuildingState(ContinuationState.ReadObjectDocument);
        }

        public void ReadNestedObject()
        {
            ref var item = ref _continuationState.PushByRef();
            item = new BuildingState(ContinuationState.ReadObject);
        }

        public void ReadProperty()
        {
            ref var item = ref _continuationState.PushByRef();
            item = new BuildingState(ContinuationState.ReadPropertyName, partialRead: true)
            {
                State = ContinuationState.ReadPropertyName,
                Properties = _propertiesCache.Allocate(),
                FirstWrite = _writer.Position,
            };
        }

        public int SizeInBytes
        {
            get
            {
                AssertNotDisposed();

                return _writer.SizeInBytes;
            }
        }

        public override void Dispose()
        {
            // We may not want to execute `.Dispose()` more than once in release, but we want to fail fast in debug if it happens.
            DisposableExceptions.ThrowIfDisposedOnDebug(this);
            
            if (_disposed.Raise() == false)
                return;

            _writer.Dispose();
            base.Dispose();
        }

        private bool ReadInternal<TWriteStrategy, TReader>(TReader reader) 
            where TWriteStrategy : IWriteStrategy
            where TReader : IJsonParser
        {
            var continuationState = _continuationState;
            var currentState = continuationState.Pop();
            var state = _state;
            while (true)
            {
                switch (currentState.State)
                {
                    case ContinuationState.ReadObjectDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.ReadObject;
                        continue;
                    case ContinuationState.ReadArrayDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
                        }

                        var fakeProperty = _context.CachedProperties.GetProperty(_fakeFieldName);
                        currentState.CurrentProperty = fakeProperty;
                        currentState.MaxPropertyId = fakeProperty.PropertyId;
                        currentState.FirstWrite = _writer.Position;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.Properties.AddByRef(new PropertyTag( property: fakeProperty ));
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
                        _propertiesCache.Return(ref currentState.Properties);
                        return true;

                    case ContinuationState.ReadObject:
                        if (state.CurrentTokenType == JsonParserToken.StartObject)
                        {
                            currentState.State = ContinuationState.ReadPropertyName;
                            currentState.Properties = _propertiesCache.Allocate();
                            currentState.FirstWrite = _writer.Position;
                            continue;
                        }

                        throw new InvalidStartOfObjectException("Expected start of object, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());

                    case ContinuationState.ReadArray:
                        if (state.CurrentTokenType == JsonParserToken.StartArray)
                        {
                            currentState.Types = _tokensCache.Allocate();
                            currentState.Positions = _positionsCache.Allocate();
                            currentState.State = ContinuationState.ReadArrayValue;
                            continue;
                        }

                        throw new InvalidStartOfObjectException("Expected start of array, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());

                    case ContinuationState.ReadArrayValue:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
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
                        _positionsCache.Return(ref currentState.Positions);
                        _tokensCache.Return(ref currentState.Types);
                        currentState = continuationState.Pop();
                        continue;

                    case ContinuationState.ReadPropertyName:
                        if (ReadMaybeModifiedPropertyName() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
                        }

                        if (state.CurrentTokenType == JsonParserToken.EndObject)
                        {
                            _modifier?.EndObject();
                            _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                            _propertiesCache.Return(ref currentState.Properties);

                            if (continuationState.Count == 0)
                                return true;

                            currentState = continuationState.Pop();
                            continue;
                        }

                        if (state.CurrentTokenType != JsonParserToken.String)
                            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());

                        var property = CreateLazyStringValueFromParserState();
                        currentState.CurrentProperty = _context.CachedProperties.GetProperty(property);
                        currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
                        currentState.State = ContinuationState.ReadPropertyValue;
                        continue;
                    case ContinuationState.ReadPropertyValue:
                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.CompleteReadingPropertyValue;
                        continuationState.Push(currentState);
                        currentState = new BuildingState(ContinuationState.ReadValue);
                        continue;
                    case ContinuationState.CompleteReadingPropertyValue:
                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        currentState.Properties.AddByRef(new PropertyTag(
                            position: _writeToken.ValuePos,
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.CurrentProperty));

                        if (currentState.PartialRead)
                        {
                            if (continuationState.Count == 0)
                            {
                                _modifier?.EndObject();
                                _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                                _propertiesCache.Return(ref currentState.Properties);
                                return true;
                            }
                        }

                        currentState.State = ContinuationState.ReadPropertyName;
                        continue;
                    case ContinuationState.ReadValue:
                        ReadJsonValue<TWriteStrategy>();
                        currentState = _continuationState.Pop();
                        break;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            AssertNotDisposed();

            if (_continuationState.Count == 0)
                return false; //nothing to do

            switch (_reader)
            {
                case ObjectJsonParser ojp when _mode == UsageMode.None: return ReadInternal<WriteNone, ObjectJsonParser>(ojp);
                case ObjectJsonParser ojp when _mode != UsageMode.None: return ReadInternal<WriteFull, ObjectJsonParser>(ojp);

                case UnmanagedJsonParser ujp when _mode == UsageMode.None: return ReadInternal<WriteNone, UnmanagedJsonParser>(ujp);
                case UnmanagedJsonParser ujp when _mode != UsageMode.None: return ReadInternal<WriteFull, UnmanagedJsonParser>(ujp);
            }
            
            return _mode == UsageMode.None ? ReadInternal<WriteNone, IJsonParser>(_reader) : ReadInternal<WriteFull, IJsonParser>(_reader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ReadMaybeModifiedPropertyName()
        {
            if (_modifier != null)
            {
                return _modifier.AboutToReadPropertyName(_reader, _state);
            }
            return _reader.Read();
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
            else if (current == JsonParserToken.Blob)
            {
                start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);
                _writeToken = new WriteToken(start, BlittableJsonToken.RawBlob);
            }
            else if (current != JsonParserToken.EndObject)
            {
                ReadJsonValueUnlikely(current);
            }
        }

        private unsafe void ReadJsonValueUnlikely(JsonParserToken current) 
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

            throw new InvalidDataException("Expected a value, but got " + current);
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

        public readonly struct WriteToken(int valuePosition, BlittableJsonToken token)
        {
            public readonly int ValuePos = valuePosition;
            public readonly BlittableJsonToken WrittenToken = token;
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
            AssertNotDisposed();

            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);
        }

        public BlittableJsonReaderObject CreateReader()
        {
            AssertNotDisposed();

            return _writer.CreateReader();
        }

        public BlittableJsonReaderArray CreateArrayReader(bool noCache)
        {
            AssertNotDisposed();

            var reader = CreateReader();
            reader.NoCache = noCache;
            if (reader.TryGet("_", out BlittableJsonReaderArray array))
            {
                array.ArrayIsRoot();
                return array;
            }
            throw new InvalidOperationException("Couldn't find array");
        }

        public override string ToString()
        {
            return "Building json for " + _debugTag;
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
