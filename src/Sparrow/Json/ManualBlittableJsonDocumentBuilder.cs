using System;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public sealed class ManualBlittableJsonDocumentBuilder<TWriter> : AbstractBlittableJsonDocumentBuilder
        where TWriter : struct, IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableWriter<TWriter> _writer;
        private UsageMode _mode;
        private WriteToken _writeToken;

        private static readonly StringSegment UnderscoreSegment = new("_");

        /// <summary>
        /// Allows incrementally building json document
        /// </summary>
        /// <param name="context"></param>
        /// <param name="mode"></param>
        /// <param name="writer"></param>
        public ManualBlittableJsonDocumentBuilder(
            JsonOperationContext context,
            UsageMode? mode = null,
            BlittableWriter<TWriter> writer = null)
        {
            _context = context;
            _mode = mode ?? UsageMode.None;
            _writer = writer ?? new BlittableWriter<TWriter>(_context);
        }

        public void StartWriteObjectDocument()
        {
            ref var state = ref _continuationState.PushByRef();
            state = new BuildingState(state: ContinuationState.ReadObjectDocument);
        }

        public void StartArrayDocument()
        {
            var fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
            var prop = _writer.CachedProperties.GetProperty(fakeFieldName);
            
            ref var state = ref _continuationState.PushByRef();
            state = new BuildingState(state: ContinuationState.ReadArrayDocument, 
                currentProperty: prop,
                maxPropertyId: prop.PropertyId, firstWrite: _writer.Position, 
                properties: _propertiesCache.Allocate());

            state.Properties.AddByRef(new PropertyTag(property: prop));
        }

        public void WritePropertyName(string propertyName)
        {
            var property = _context.GetLazyStringForFieldWithCaching(propertyName);
            WritePropertyName(property);
        }

        public void WritePropertyName(LazyStringValue property)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            if (currentState.State != ContinuationState.ReadPropertyName)
            {
                ThrowIllegalStateException(currentState.State, "WritePropertyName");
            }

            currentState.CurrentProperty = _writer.CachedProperties.GetProperty(property);
            currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
            currentState.State = ContinuationState.ReadPropertyValue;
        }

        public void StartWriteObject()
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var previousState = ref _continuationState.TopByRef();

            if (previousState.State != ContinuationState.ReadObjectDocument &&
                previousState.State != ContinuationState.ReadPropertyValue &&
                previousState.State != ContinuationState.ReadArray)
                ThrowIllegalStateException(previousState.State, "WriteObject");

            previousState.State = previousState.State == ContinuationState.ReadPropertyValue ? ContinuationState.ReadPropertyName : previousState.State;

            ref var state = ref _continuationState.PushByRef();
            state = new BuildingState(state: ContinuationState.ReadPropertyName, properties: _propertiesCache.Allocate(), firstWrite: -1);
        }

        public void WriteObjectEnd()
        {
            var currentState = _continuationState.Pop();
            long start = _writer.Position;
            switch (currentState.State)
            {
                case ContinuationState.ReadPropertyName:
                case ContinuationState.ReadPropertyValue:
                    {
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite,
                            currentState.MaxPropertyId);

                        _propertiesCache.Return(ref currentState.Properties);

                        // here we know that the last item in the stack is the keep the last ReadObjectDocument
                        if (_continuationState.Count > 1)
                        {
                            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
                            ref var outerState = ref _continuationState.TopByRef();
                            if (outerState.State == ContinuationState.ReadArray)
                            {
                                outerState.Types.Add(_writeToken.WrittenToken);
                                outerState.Positions.Add(_writeToken.ValuePos);
                            }
                            else
                            {
                                outerState.Properties.AddByRef(new PropertyTag(
                                    type: (byte)_writeToken.WrittenToken,
                                    property: outerState.CurrentProperty,
                                    position: _writeToken.ValuePos
                                ));
                            }

                            if (outerState.FirstWrite == -1)
                                outerState.FirstWrite = start;
                        }
                    }
                    break;

                case ContinuationState.ReadArray:
                    {
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite,
                            currentState.MaxPropertyId);

                        _propertiesCache.Return(ref currentState.Properties);

                        if (_continuationState.Count > 1)
                        {
                            var outerState = _continuationState.Count > 0 ? _continuationState.Pop() : currentState;
                            if (outerState.FirstWrite == -1)
                                outerState.FirstWrite = start;
                            _continuationState.Push(outerState);
                        }

                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        _continuationState.Push(currentState);
                    }
                    break;

                case ContinuationState.ReadObjectDocument:
                    {
                        currentState.Properties.AddByRef(new PropertyTag(
                            position: _writeToken.ValuePos,
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.CurrentProperty)
                        );
                        
                        if (currentState.FirstWrite == -1)
                            currentState.FirstWrite = start;

                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                        _propertiesCache.Return(ref currentState.Properties);
                    }
                    break;

                default:
                    ThrowIllegalStateException(currentState.State, "ReadEndObject");
                    break;
            }
        }

        public void StartWriteArray()
        {
            _continuationState.Push(new BuildingState(
                state: ContinuationState.ReadArray,
                types: _tokensCache.Allocate(),
                positions: _positionsCache.Allocate())
            );
        }

        public void WriteArrayEnd()
        {
            var currentState = _continuationState.Pop();

            switch (currentState.State)
            {
                case ContinuationState.ReadArrayDocument:
                    currentState.Properties[0] = new PropertyTag(
                        property: currentState.Properties[0].Property,
                        type: (byte)_writeToken.WrittenToken,
                        position: _writeToken.ValuePos);

                    // Register property position, name id (PropertyId) and type (object type and metadata)
                    _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                    _continuationState.Push(currentState);
                    break;

                case ContinuationState.ReadArray:
                    var arrayToken = BlittableJsonToken.StartArray;
                    var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types, ref arrayToken);
                    _positionsCache.Return(ref currentState.Positions);
                    _tokensCache.Return(ref currentState.Types);

                    _writeToken = new WriteToken(valuePosition: arrayInfoStart, token: arrayToken);

                    if (_continuationState.Count >= 1)
                    {
                        // PERF: We are going to be popping and pushing, which is essentially modifying the top.
                        ref var outerState = ref _continuationState.TopByRef();

                        if (outerState.FirstWrite == -1)
                            outerState.FirstWrite = arrayInfoStart;

                        switch (outerState.State)
                        {
                            case ContinuationState.ReadPropertyName:
                            case ContinuationState.ReadPropertyValue:
                                outerState.Properties.AddByRef(new PropertyTag(
                                    type: (byte)_writeToken.WrittenToken,
                                    property: outerState.CurrentProperty,
                                    position: _writeToken.ValuePos
                                ));
                                outerState.State = ContinuationState.ReadPropertyName;
                                break;
                            case ContinuationState.ReadArray:
                                outerState.Types.Add(_writeToken.WrittenToken);
                                outerState.Positions.Add(_writeToken.ValuePos);
                                break;
                            case ContinuationState.ReadArrayDocument:
                                outerState.Properties[0] = new PropertyTag(
                                    type: (byte)_writeToken.WrittenToken,
                                    property: outerState.Properties[0].Property,
                                    position: _writeToken.ValuePos
                                );

                                // Register property position, name id (PropertyId) and type (object type and metadata)
                                _writeToken = _writer.WriteObjectMetadata(outerState.Properties, outerState.FirstWrite, outerState.MaxPropertyId);
                                break;
                            default:
                                throw new InvalidOperationException($"Cannot perform {outerState.State} when encountered the ReadEndArray state");
                        }
                    }
                    break;

                default:
                    ThrowIllegalStateException(currentState.State, "ReadEndArray");
                    break;
            }
        }

        public void WriteValueNull()
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteNull();
            _writeToken = new WriteToken(valuePosition: valuePos, token: BlittableJsonToken.Null);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(BlittableJsonToken token, object value)
        {
            switch (token)
            {
                case BlittableJsonToken.Integer:
                    WriteValue((long)value);
                    break;

                case BlittableJsonToken.LazyNumber:
                    WriteValue((LazyNumberValue)value);
                    break;

                case BlittableJsonToken.String:
                    WriteValue((LazyStringValue)value);
                    break;

                case BlittableJsonToken.CompressedString:
                    WriteValue((LazyCompressedStringValue)value);
                    break;

                case BlittableJsonToken.Boolean:
                    WriteValue((bool)value);
                    break;

                case BlittableJsonToken.Null:
                    WriteValueNull();
                    break;

                case BlittableJsonToken.StartObject:
                    var obj = value as BlittableJsonReaderObject;
                    StartWriteObject();
                    obj.AddItemsToStream(this);
                    WriteObjectEnd();
                    break;

                case BlittableJsonToken.EmbeddedBlittable:
                    WriteEmbeddedBlittableDocument((BlittableJsonReaderObject)value);
                    break;

                case BlittableJsonToken.StartArray:
                    var arr = value as BlittableJsonReaderArray;
                    StartWriteArray();
                    arr?.AddItemsToStream(this);
                    WriteArrayEnd();
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(token), token, null);
            }
        }

        public void WriteValue(bool value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.Boolean);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(long value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.Integer);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(float value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.LazyNumber);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(ulong value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.LazyNumber);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(double value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();
            
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.LazyNumber);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(decimal value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.LazyNumber);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyNumberValue value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.LazyNumber);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(string value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();
            
            var valuePos = _writer.WriteValue(value, out BlittableJsonToken stringToken, _mode);
            _writeToken = new WriteToken(valuePosition: valuePos, stringToken);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyStringValue value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value, out BlittableJsonToken stringToken, UsageMode.None, null);
            _writeToken = new WriteToken(valuePosition: valuePos, stringToken);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyCompressedStringValue value)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();

            var valuePos = _writer.WriteValue(value, out BlittableJsonToken stringToken, UsageMode.None);
            _writeToken = new WriteToken(valuePosition: valuePos, stringToken);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public unsafe void WriteEmbeddedBlittableDocument(BlittableJsonReaderObject document)
        {
            WriteEmbeddedBlittableDocument(document.BasePointer, document.Size);
        }

        public unsafe void WriteRawBlob(byte* ptr, int size)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();
            
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.RawBlob);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public unsafe void WriteEmbeddedBlittableDocument(byte* ptr, int size)
        {
            // PERF: We are going to be popping and pushing, which is essentially modifying the top.
            ref var currentState = ref _continuationState.TopByRef();
            
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken(valuePosition: valuePos, BlittableJsonToken.EmbeddedBlittable);

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        private void FinishWritingScalarValue(ref BuildingState currentState)
        {
            switch (currentState.State)
            {
                case ContinuationState.ReadPropertyValue:
                    currentState.Properties.AddByRef(new PropertyTag(
                        position: _writeToken.ValuePos,
                        type: (byte)_writeToken.WrittenToken,
                        property: currentState.CurrentProperty
                    ));

                    currentState.State = ContinuationState.ReadPropertyName;
                    break;

                case ContinuationState.ReadArray:
                    currentState.Types.Add(_writeToken.WrittenToken);
                    currentState.Positions.Add(_writeToken.ValuePos);
                    break;

                default:
                    ThrowIllegalStateException(currentState.State, "ReadValue");
                    break;
            }
        }

        public void FinalizeDocument()
        {
            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);

            ClearState();
        }

        private void ThrowIllegalStateException(ContinuationState state, string realOperation)
        {
            throw new InvalidOperationException($"Cannot perform {realOperation} when encountered the {state} state");
        }

        public void Reset(UsageMode mode)
        {
            _mode = mode;
            _writer.ResetAndRenew();
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _writer.CreateReader();
        }

        public BlittableJsonReaderArray CreateArrayReader()
        {
            var reader = CreateReader();
            if (reader.TryGet("_", out BlittableJsonReaderArray array))
                return array;
            
            throw new InvalidOperationException("Couldn't find array");
        }

        public override void Dispose()
        {
            _writer.Dispose();
            base.Dispose();
        }
    }
}
