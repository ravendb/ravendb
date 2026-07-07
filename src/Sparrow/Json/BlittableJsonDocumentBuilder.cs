using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Exceptions;
using Sparrow.Json.Parsing;
using Sparrow.Threading;

namespace Sparrow.Json
{
    public sealed unsafe class BlittableJsonDocumentBuilder : AbstractBlittableJsonDocumentBuilder
    {
        private static readonly StringSegment UnderscoreSegment = new StringSegment("_");

        private readonly JsonOperationContext _context;
        private UsageMode _mode;
        private readonly IJsonParser _reader;
        public IBlittableDocumentModifier _modifier;

        // We introduced a delegate that encapsulates the reading logic.
        // This avoids branching on every 'Read()' call to figure out which path to take
        // (WriteNone vs. WriteFull, ObjectJsonParser vs. UnmanagedJsonParser).
        // Instead, we decide once in the constructor or Renew(...) and store that strategy here.
        private Func<bool> _readInternalFunc;

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

            // Here we bind the specialized read function once, based on the current parser and mode.
            // This helps eliminate per-call conditionals in Read().
            _readInternalFunc = GetReadInternalFunction(_reader, _mode);
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

        private Func<bool> GetReadInternalFunction(IJsonParser reader, UsageMode mode)
        {
            // Instead of checking mode and parser type repeatedly in the reading loop,
            // we perform that check once. We return a specialized generic function
            // (either ReadInternal<WriteNone, TParser> or ReadInternal<WriteFull, TParser>).
            // This approach enables better inlining and reduces branching inside Read().
            return mode switch
            {
                UsageMode.None => reader switch
                {
                    ObjectJsonParser => ReadInternal<WriteNone, ObjectJsonParser, NoStreaming>,
                    UnmanagedJsonParser => ReadInternal<WriteNone, UnmanagedJsonParser, NoStreaming>,
                    _ => ReadInternal<WriteNone, IJsonParser, NoStreaming>
                },
                UsageMode.ForStreaming => reader switch
                {
                    UnmanagedJsonParser => ReadInternal<WriteNone, UnmanagedJsonParser, WithStreaming>,
                    _ => throw new NotSupportedException(),
                },
                _ => reader switch
                {
                    ObjectJsonParser => ReadInternal<WriteFull, ObjectJsonParser, NoStreaming>,
                    UnmanagedJsonParser => ReadInternal<WriteFull, UnmanagedJsonParser, NoStreaming>,
                    _ => ReadInternal<WriteFull, IJsonParser, NoStreaming>
                }
            };
        }

        public void Reset()
        {
            AssertNotDisposed();

            _debugTag = null;
            _mode = UsageMode.None;
            _readInternalFunc = null;

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

            // Renew is now also responsible for re-binding the read function
            // if the UsageMode changes. This resets parsing state and ensures
            // we're using the correct specialized read routine going forward.
            _readInternalFunc = GetReadInternalFunction(_reader, mode);

            ClearState();

            _writer.ResetAndRenew();
            _modifier?.Reset(_context);

            _fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
        }

        public void ReadArrayDocument()
        {
            AssertNotDisposed();

            _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadArrayDocument);
        }

        public void ReadObjectDocument()
        {
            AssertNotDisposed();

            _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadObjectDocument);
        }

        public void ReadNestedObject()
        {
            AssertNotDisposed();

            _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadObject);
        }

        public void ReadProperty()
        {
            AssertNotDisposed();

            _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadPropertyName)
            {
                State = ContinuationState.ReadPropertyName,
                Properties = _propertiesCache.Allocate(),
                FirstWrite = _writer.Position,
                PartialRead = true
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
            if (_disposed.Raise() == false)
                return;

            _writer.Dispose();
            base.Dispose();
        }

        public (string Name, Action<UnmanagedWriteBuffer> Handler) PropertyToWatchForStreaming; 

        private bool ReadInternal<TWriteStrategy, TJsonParser, TStreamBehavior>() 
            where TWriteStrategy : IWriteStrategy
            where TJsonParser : IJsonParser
            where TStreamBehavior : IStreamingBehavior 
        {
            CachedProperties.PropertyName fakeProperty = null;

            // PERF: This method is performance critical, therefore, we replaced
            // '_continuationState.Push(...)' with 'PushByRef() = ...' to avoid unnecessary copying
            // of 'BuildingState' structs and reduce overhead.
            // This small change can yield noticeable performance improvements under heavy usage.

            var continuationState = _continuationState;
            var currentState = continuationState.Pop();
            var reader = (TJsonParser) _reader;
            var state = _state;
            while (true)
            {
                switch (currentState.State)
                {
                    case ContinuationState.ReadPropertyValue:
                        if (reader.Read() == false)
                        {
                            continuationState.PushByRef() = currentState;
                            return false;
                        }
                        currentState.State = ContinuationState.CompleteReadingPropertyValue;
                        continuationState.PushByRef() = currentState;

                        goto case ContinuationState.ReadValue;

                    case ContinuationState.ReadValue:
                        ReadJsonValue<TWriteStrategy, TJsonParser>();
                        currentState = _continuationState.Pop();
                        continue;

                    case ContinuationState.ReadPropertyName:
                        if (ReadMaybeModifiedPropertyName(reader))
                        {
                            if (state.CurrentTokenType != JsonParserToken.EndObject)
                            {
                                if (state.CurrentTokenType == JsonParserToken.String)
                                {
                                    var property = CreateLazyStringValueFromParserState();

                                    if(typeof(TStreamBehavior) == typeof(WithStreaming) && property.Equals(PropertyToWatchForStreaming.Name))
                                    {
                                        Action<UnmanagedWriteBuffer> handler = PropertyToWatchForStreaming.Handler;
                                        reader.OnStringRead = (buffer, partial) =>
                                        {
                                            handler(buffer);
                                            if (partial is false) // we are done...
                                                reader.OnStringRead = null;
                                        };
                                        PropertyToWatchForStreaming = default; // we only do that for the _first_ property that match the name
                                    }
                                    
                                    currentState.CurrentProperty = _context.CachedProperties.GetProperty(property);
                                    currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
                                    currentState.State = ContinuationState.ReadPropertyValue;

                                    // PERF: _isVectorProperty is now set by CurrentProperty.IsVectorProperty,
                                    // removing a string comparison each time. We rely on a cached boolean 
                                    // from the property lookup to decide if we should read buffered vector data.
                                    _isVectorProperty = currentState.CurrentProperty.IsVectorProperty;

                                    goto case ContinuationState.ReadPropertyValue;
                                }
                                
                                ThrowExpectedProperty();
                            }

                            _modifier?.EndObject();
                            _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                            _propertiesCache.Return(ref currentState.Properties);

                            if (continuationState.Count == 0)
                                return true;

                            currentState = continuationState.Pop();
                            continue;
                        }

                        continuationState.PushByRef() = currentState;
                        return false;
                        
                    case ContinuationState.ReadObjectDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.PushByRef() = currentState;
                            return false;
                        }
                        currentState.State = ContinuationState.ReadObject;
                        goto case ContinuationState.ReadObject;

                    case ContinuationState.ReadObject:
                        if (state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowExpectedStartOfObject();

                        currentState.State = ContinuationState.ReadPropertyName;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.FirstWrite = _writer.Position;
                        goto case ContinuationState.ReadPropertyName;

                    case ContinuationState.CompleteReadingPropertyValue:
                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        currentState.Properties.Add(new PropertyTag(
                            position: _writeToken.ValuePos,
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.CurrentProperty));

                        if (currentState.PartialRead && continuationState.Count == 0)
                        {
                            _modifier?.EndObject();
                            _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                            _propertiesCache.Return(ref currentState.Properties);
                            return true;
                        }

                        currentState.State = ContinuationState.ReadPropertyName;
                        goto case ContinuationState.ReadPropertyName;

                    case ContinuationState.ReadArrayDocument:
                        if (reader.Read() == false)
                        {
                            continuationState.PushByRef() = currentState;
                            return false;
                        }

                        fakeProperty ??= _context.CachedProperties.GetProperty(_fakeFieldName);
                        currentState.CurrentProperty = fakeProperty;
                        currentState.MaxPropertyId = fakeProperty.PropertyId;
                        currentState.FirstWrite = _writer.Position;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.Properties.Add(new PropertyTag(fakeProperty));
                        currentState.State = ContinuationState.CompleteDocumentArray;
                        continuationState.PushByRef() = currentState;
                        currentState = new BuildingState(ContinuationState.ReadArray);
                        goto case ContinuationState.ReadArray;

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

                    case ContinuationState.ReadArray:
                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowExpectedStartOfArray();

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
                            continuationState.PushByRef() = currentState;
                            return false;
                        }

                        if (state.CurrentTokenType == JsonParserToken.EndArray)
                        {
                            currentState.State = ContinuationState.CompleteArray;
                            goto case ContinuationState.CompleteArray;
                        }

                        currentState.State = ContinuationState.CompleteArrayValue;
                        continuationState.PushByRef() = currentState;
                        goto case ContinuationState.ReadValue;

                    case ContinuationState.CompleteArrayValue:
                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        currentState.State = ContinuationState.ReadArrayValue;
                        goto case ContinuationState.ReadArrayValue;

                    case ContinuationState.CompleteArray:
                        var arrayToken = BlittableJsonToken.StartArray;
                        var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types, ref arrayToken);
                        _writeToken = new WriteToken(arrayInfoStart, arrayToken);
                        _positionsCache.Return(ref currentState.Positions);
                        _tokensCache.Return(ref currentState.Types);
                        currentState = continuationState.Pop();
                        continue;
                    
                    case ContinuationState.ReadBufferedArrayValue:

                        // The same approach of buffered vectors is used, but we streamlined the
                        // transition to/from "vector reading mode" by short-circuiting and returning
                        // to normal array reading if any token doesn't fit the vector requirements.
                        // This reduces overhead for large numeric arrays or mixed-type arrays.
                        if (reader.Read() == false)
                        {
                            continuationState.PushByRef() = currentState;
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
                                        reader.ValidateFloat();

                                    _state.AddBuffered(decimal.ToDouble(value));
                                }
                                processed = true;
                                break;

#else
                                throw new NotSupportedException("Vector representations are only supported for .NET versions greater than 7.0");
#endif
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
                        continuationState.PushByRef() = currentState;

                        // Allow the loop to continue to the next iteration
                        goto case ContinuationState.ReadValue;

                    case ContinuationState.CompleteBufferedArray:
                        int startPos = WriteBufferedVector();
                        _writeToken = new WriteToken(startPos, BlittableJsonToken.Vector);

                        state.ClearBuffered();
                        currentState = _continuationState.Pop();
                        continue;
                }
            }
        }

        private struct VectorProcessor<T> where T : unmanaged
        {
            internal static int ProcessVector(byte* buffer, int size, JsonParserState state, BlittableWriter<UnmanagedWriteBuffer> writer)
            {
                Span<T> st = new(buffer, size);
                int count = state.FillVector(st);
                return writer.WriteVector<T>(st.Slice(0, count));
            }
        }

        private int WriteBufferedVector()
        {
            int count = _state._bufferedSequence.Count;
            int size = count * sizeof(long);
            using var _ = _context.GetMemoryBuffer(size, out var buffer);

            var type = _state.GetBufferedOptimalType();

            // Additional small but impactful detail:
            // Using 'switch (type)' with specialized method calls (VectorProcessor<double>, etc.)
            // helps the JIT inline the vector writing logic for each numeric type more effectively.
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

            return _readInternalFunc();
        }

        private bool ReadMaybeModifiedPropertyName<TJsonParser>(TJsonParser reader)
            where TJsonParser : IJsonParser
        {
            if (_modifier == null) 
                return reader.Read();

            return _modifier.AboutToReadPropertyName(reader, _state);
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private void ThrowExpectedProperty()
        {
            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private void ThrowExpectedStartOfArray()
        {
            throw new InvalidStartOfObjectException("Expected start of array, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private void ThrowExpectedStartOfObject()
        {
            throw new InvalidStartOfObjectException("Expected start of object, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }
        
        private interface IStreamingBehavior{}

        private struct NoStreaming : IStreamingBehavior{}
        
        private struct WithStreaming : IStreamingBehavior{}

        private interface IWriteStrategy { }
        
        private struct WriteFull : IWriteStrategy { }

        private struct WriteNone : IWriteStrategy { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReadJsonValue<TWriteStrategy, TJsonParser>() 
            where TWriteStrategy : IWriteStrategy
            where TJsonParser : IJsonParser
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
                _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadObject);
            }
            else if (current != JsonParserToken.EndObject)
            {
                ReadJsonValueUnlikely<TJsonParser>(current);
            }
        }

        private void ReadJsonValueUnlikely<TJsonParser>(JsonParserToken current) 
            where TJsonParser : IJsonParser
        {
            int start;
            switch (current)
            {
                case JsonParserToken.StartArray:
                    _continuationState.PushByRef() = new BuildingState(ContinuationState.ReadArray);
                    return;

                case JsonParserToken.Float:
                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                        ((TJsonParser)_reader).ValidateFloat();

                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);

                    _state.CompressedSize = null;
                    _writeToken = new WriteToken(start, BlittableJsonToken.LazyNumber);
                    return;

                case JsonParserToken.True:
                case JsonParserToken.False:
                    start = _writer.WriteValue(current == JsonParserToken.True ? (byte)1 : (byte)0);
                    _writeToken = new WriteToken(start, BlittableJsonToken.Boolean);
                    return;

                case JsonParserToken.Blob:
                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize);
                    _writeToken = new WriteToken(start, BlittableJsonToken.RawBlob);
                    return;

                case JsonParserToken.Null:
                    // nothing to do here, we handle that with the token
                    start = _writer.WriteValue((byte)0);
                    _writeToken = new WriteToken(start, BlittableJsonToken.Null);
                    return;
            }

            ThrowExpectedValue(current);
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
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
            ToDisk = ValidateDouble | CompressStrings,
            ForStreaming = 32
        }

        public struct WriteToken(int position, BlittableJsonToken token)
        {
            public int ValuePos = position;
            public BlittableJsonToken WrittenToken = token;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe LazyStringValue CreateLazyStringValueFromParserState()
        {
            var lazyStringValueFromParserState = _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize);
            lazyStringValueFromParserState.EscapePositions = _state.EscapePositions.Count > 0 ? _state.EscapePositions.ToArray() : [];
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
            if (reader.TryGet(BlittableJsonReaderArray.RootArrayHolderPropertyNameSegment, out BlittableJsonReaderArray array))
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
