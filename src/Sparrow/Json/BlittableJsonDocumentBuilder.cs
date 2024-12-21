using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Exceptions;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Sparrow.Json
{
    public sealed class BlittableJsonDocumentBuilder : AbstractBlittableJsonDocumentBuilder
    {
        private static readonly StringSegment UnderscoreSegment = new StringSegment("_");

        private readonly JsonOperationContext _context;
        private UsageMode _mode;
        private readonly IJsonParser _reader;

        public IBlittableDocumentModifier _modifier;
        private readonly BlittableWriter<UnmanagedWriteBuffer> _writer;
        private readonly JsonParserState _state;
        private LazyStringValue _fakeFieldName;

        private readonly SingleUseFlag _disposed = new SingleUseFlag();

        private WriteToken _writeToken;
        private string _debugTag;

        private bool _isVectorProperty;

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

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, UsageMode mode, string debugTag, IJsonParser reader, BlittableWriter<UnmanagedWriteBuffer> writer = null) : this(context, state, reader, writer)
        {
            Renew(debugTag, mode);
        }

        public void Reset()
        {
            AssertNotDisposed();

            _debugTag = null;
            _mode = UsageMode.None;

            ClearState();

            _writeToken = default;
            _writer.Reset();
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
            AssertNotDisposed();

            _continuationState.Push(new BuildingState(ContinuationState.ReadArrayDocument));
        }

        public void ReadObjectDocument()
        {
            AssertNotDisposed();

            _continuationState.Push(new BuildingState(ContinuationState.ReadObjectDocument));
        }

        public void ReadNestedObject()
        {
            AssertNotDisposed();

            _continuationState.Push(new BuildingState(ContinuationState.ReadObject));
        }

        public void ReadProperty()
        {
            AssertNotDisposed();

            var state = new BuildingState(ContinuationState.ReadPropertyName)
            {
                State = ContinuationState.ReadPropertyName,
                Properties = _propertiesCache.Allocate(),
                FirstWrite = _writer.Position,
                PartialRead = true
            };
            _continuationState.Push(state);
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
            if (_disposed.Raise() == false)
                return;

            _writer.Dispose();
            base.Dispose();
        }

        private unsafe bool ReadInternal<TWriteStrategy>() where TWriteStrategy : IWriteStrategy
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
                        currentState.Properties.Add(new PropertyTag { Property = fakeProperty });
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
                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            goto ErrorExpectedStartOfArray;

                        currentState.Types = _tokensCache.Allocate();
                        currentState.Positions = _positionsCache.Allocate();
                        
                        if (_isVectorProperty == false)
                        {
                            currentState.State = ContinuationState.ReadArrayValue;
                            goto case ContinuationState.ReadArrayValue;
                        }

                        _isVectorProperty = false;
                        currentState.State = ContinuationState.ReadBufferedArrayValue;
                        goto case ContinuationState.ReadBufferedArrayValue;

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
                        _positionsCache.Return(ref currentState.Positions);
                        _tokensCache.Return(ref currentState.Types);
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
                            _propertiesCache.Return(ref currentState.Properties);

                            if (continuationState.Count == 0)
                                goto ReturnTrue;

                            currentState = continuationState.Pop();
                            continue;
                        }

                        if (state.CurrentTokenType != JsonParserToken.String)
                            goto ErrorExpectedProperty;

                        var property = CreateLazyStringValueFromParserState();
                        currentState.CurrentProperty = _context.CachedProperties.GetProperty(property);

                        if (currentState.CurrentProperty.EqualsPropertyNameToBytes(Global.Constants.Naming.VectorPropertyNameAsSpan))
                            _isVectorProperty = true;

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
                        currentState.Properties.Add(new PropertyTag(
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
                                goto ReturnTrue;
                            }
                        }

                        currentState.State = ContinuationState.ReadPropertyName;
                        continue;
                    case ContinuationState.ReadBufferedArrayValue:

                        if (reader.Read() == false)
                        {
                            continuationState.Push(currentState);
                            return false;
                        }

                        if (state.CurrentTokenType == JsonParserToken.EndArray)
                        {
                            if (_state._bufferedSequence?.Count > 0)
                            {
                                currentState.State = ContinuationState.CompleteBufferedArray;
                                goto case ContinuationState.CompleteBufferedArray;
                            }

                            currentState.State = ContinuationState.CompleteArray;
                            goto case ContinuationState.CompleteArray;
                        }

                        bool processed = false;

                        // We try to read integers and doubles, if the value is a decimal (which can happen)
                        // then we bail out because we cannot handle it as a vector (as least for now). 
                        JsonParserToken current = _state.CurrentTokenType;

                        switch (current)
                        {
                            case JsonParserToken.Integer:
                                _state.AddBuffered(_state.Long);
                                processed = true;
                                break;
                            case JsonParserToken.Float:
                                var numberString = new ReadOnlySpan<byte>(_state.StringBuffer, _state.StringSize);
                                if (Utf8Parser.TryParse(numberString, out decimal value, out int bytesConsumed) == false)
                                {
                                    //We suspect the underlying value might be a double with large values or exponents, so let's verify it again before moving to classic array.
                                    if (Utf8Parser.TryParse(numberString, out double dValue, out bytesConsumed) == false)
                                    {
                                        break;
                                    }
                                    
                                    _state.AddBuffered(dValue);
                                    processed = true;
                                    break;
                                }

                                Debug.Assert(bytesConsumed == _state.StringSize);
#if NET7_0_OR_GREATER
                                // This will only be executed on the server, so no problem on this regard as vector
                                // representations are not supported on older systems.
                                if (decimal.IsInteger(value) && decimal.Abs(value) < long.MaxValue)
                                {
                                    _state.AddBuffered(decimal.ToInt64(value));
                                }
                                else
                                {
                                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                                        _reader.ValidateFloat();

                                    _state.AddBuffered(decimal.ToDouble(value));
                                }
#else
                                throw new NotSupportedException("Vector representations are only supported for .Net versions greater than 6.0");
#endif
                                processed = true;
                                break;
                        }

                        if (processed)
                            goto case ContinuationState.ReadBufferedArrayValue; // Successfully read a buffered value, continue buffering

                        // Process the buffered sequence
                        if (_state._bufferedSequence is { Count: > 0 })
                        {
                            // Write out buffered values
                            foreach ((var token, long asLong) in _state._bufferedSequence)
                            {
                                int position;
                                BlittableJsonToken tokenToWrite;

                                switch (token)
                                {
                                    case JsonParserToken.Integer:
                                        position = _writer.WriteValue(asLong);
                                        tokenToWrite = BlittableJsonToken.Integer;
                                        break;
                                    case JsonParserToken.Float:
                                        long asRef = asLong;
                                        position = _writer.WriteValue(Unsafe.As<long, double>(ref asRef));
                                        tokenToWrite = BlittableJsonToken.LazyNumber;
                                        break;
                                    default:
                                        throw new NotSupportedException($"Unsupported token type: {token}");
                                }

                                currentState.Types.Add(tokenToWrite);
                                currentState.Positions.Add(position);
                            }

                            // Clear the buffered sequence
                            _state.ClearBuffered();
                        }

                        // Now handle the current value that couldn't be buffered.

                        // The change of the current state must happen before as ReadJsonValue may return a new instance.
                        currentState.State = ContinuationState.CompleteArrayValue;
                        ReadJsonValue<TWriteStrategy>();

                        // Allow the loop to continue to the next iteration
                        continue;

                    case ContinuationState.CompleteBufferedArray:
                        int startPos = WriteBufferedVector();
                        _writeToken = new WriteToken(startPos, BlittableJsonToken.Vector);

                        state.ClearBuffered();
                        currentState = _continuationState.Pop();
                        continue;

                    case ContinuationState.ReadValue:
                        ReadJsonValue<TWriteStrategy>();
                        currentState = _continuationState.Pop();
                        break;
                }
            }

        ReturnTrue:
            return true;
        ReturnFalse:
            return false;

        ErrorExpectedProperty:
            ThrowExpectedProperty();
        ErrorExpectedStartOfObject:
            ThrowExpectedStartOfObject();
        ErrorExpectedStartOfArray:
            ThrowExpectedStartOfArray();
            return false; // Will never execute.
        }

        private struct VectorProcessor<T> where T : unmanaged
        {
            internal static unsafe int ProcessVector(byte* buffer, int size, JsonParserState state, BlittableWriter<UnmanagedWriteBuffer> writer)
            {
                Span<T> st = new(buffer, size);
                int count = state.FillVector(st);
                return writer.WriteVector<T>(st.Slice(0, count));
            }
        }

        private unsafe int WriteBufferedVector()
        {
            int count = _state._bufferedSequence.Count;
            int size = count * sizeof(long);
            using var _ = _context.GetMemoryBuffer(size, out var buffer);

            var type = _state.GetBufferedOptimalType();
            switch (type)
            {
                case BlittableVectorType.Double:
                    return VectorProcessor<double>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.Float:
                    return VectorProcessor<float>.ProcessVector(buffer.Address, count, _state, _writer);

#if NET6_0_OR_GREATER
                case BlittableVectorType.Half:
                    return VectorProcessor<Half>.ProcessVector(buffer.Address, count, _state, _writer);
#endif
                case BlittableVectorType.Byte:
                    return VectorProcessor<byte>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.SByte:
                    return VectorProcessor<sbyte>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.Int16:
                    return VectorProcessor<short>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.UInt16:
                    return VectorProcessor<ushort>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.Int32:
                    return VectorProcessor<int>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.UInt32:
                    return VectorProcessor<uint>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.Int64:
                    return VectorProcessor<long>.ProcessVector(buffer.Address, count, _state, _writer);

                case BlittableVectorType.UInt64:
                    return VectorProcessor<ulong>.ProcessVector(buffer.Address, count, _state, _writer);
            }

            throw new NotSupportedException($"The type {type} is not supported.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            AssertNotDisposed();

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
            else if (current == JsonParserToken.Blob)
            {
                start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);
                _writeToken = new WriteToken(start, BlittableJsonToken.RawBlob);
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
