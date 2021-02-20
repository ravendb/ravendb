using System;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
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

        private static readonly StringSegment UnderscoreSegment = new StringSegment("_");

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
            state.State = ContinuationState.ReadObjectDocument;
        }

        public void StartArrayDocument()
        {
            var fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
            var prop = _writer.CachedProperties.GetProperty(fakeFieldName);

            ref var currentState = ref _continuationState.PushByRef();
            currentState.State = ContinuationState.ReadArrayDocument;
            currentState.CurrentProperty = prop;
            currentState.MaxPropertyId = prop.PropertyId;
            currentState.FirstWrite = _writer.Position;
            currentState.Properties = _propertiesCache.Allocate();

            ref var tag = ref currentState.Properties.AddAsRef();
            tag.Property = prop;
        }

        public void WritePropertyName(string propertyName)
        {
            var property = _context.GetLazyStringForFieldWithCaching(propertyName);
            WritePropertyName(property);
        }

        public void WritePropertyName(LazyStringValue property)
        {
            ref var currentState = ref _continuationState.PeekByRef();
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
            ref var previousState = ref _continuationState.PeekByRef();

            if (previousState.State != ContinuationState.ReadObjectDocument &&
                previousState.State != ContinuationState.ReadPropertyValue &&
                previousState.State != ContinuationState.ReadArray)
                ThrowIllegalStateException(previousState.State, "WriteObject");

            previousState.State = (previousState.State == ContinuationState.ReadPropertyValue) ? ContinuationState.ReadPropertyName : previousState.State;

            ref var nextState = ref _continuationState.PushByRef();
            nextState.State = ContinuationState.ReadPropertyName;
            nextState.Properties = _propertiesCache.Allocate();
            nextState.FirstWrite = -1;
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
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);

                        _propertiesCache.Return(ref currentState.Properties);

                        // here we know that the last item in the stack is the keep the last ReadObjectDocument
                        if (_continuationState.Count > 1)
                        {
                            ref var outerState = ref _continuationState.PeekByRef();
                            if (outerState.State == ContinuationState.ReadArray)
                            {
                                outerState.Types.Add(_writeToken.WrittenToken);
                                outerState.Positions.Add(_writeToken.ValuePos);
                            }
                            else
                            {
                                ref var tag = ref outerState.Properties.AddAsRef();
                                tag.Type = (byte)_writeToken.WrittenToken;
                                tag.Property = outerState.CurrentProperty;
                                tag.Position = _writeToken.ValuePos;
                            }

                            if (outerState.FirstWrite == -1)
                                outerState.FirstWrite = start;
                        }
                    }
                    break;

                case ContinuationState.ReadArray:
                    {
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);

                        _propertiesCache.Return(ref currentState.Properties);

                        if (_continuationState.Count > 1)
                        {
                            ref var outerState = ref _continuationState.PeekByRef();                           
                            if (outerState.FirstWrite == -1)
                                outerState.FirstWrite = start;
                        }

                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        _continuationState.Push(currentState);
                    }
                    break;

                case ContinuationState.ReadObjectDocument:
                    {
                        ref var tag = ref currentState.Properties.AddAsRef();
                        tag.Type = (byte)_writeToken.WrittenToken;
                        tag.Property = currentState.CurrentProperty;
                        tag.Position = _writeToken.ValuePos;

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
            ref var state = ref _continuationState.PushByRef();
            state.State = ContinuationState.ReadArray;
            state.Types = _tokensCache.Allocate();
            state.Positions = _positionsCache.Allocate();
        }

        public void WriteArrayEnd()
        {
            var currentState = _continuationState.Pop();

            switch (currentState.State)
            {
                case ContinuationState.ReadArrayDocument:
                    ref var tag = ref currentState.Properties.GetAsRef(0);
                    tag.Type = (byte)_writeToken.WrittenToken;
                    tag.Property = currentState.Properties[0].Property;
                    tag.Position = _writeToken.ValuePos;

                    // Register property position, name id (PropertyId) and type (object type and metadata)
                    _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                    _continuationState.Push(currentState);
                    break;

                case ContinuationState.ReadArray:
                    var arrayToken = BlittableJsonToken.StartArray;
                    var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types, ref arrayToken);
                    _positionsCache.Return(ref currentState.Positions);
                    _tokensCache.Return(ref currentState.Types);

                    _writeToken = new WriteToken
                    {
                        ValuePos = arrayInfoStart,
                        WrittenToken = arrayToken
                    };

                    if (_continuationState.Count >= 1)
                    {
                        ref var outerState = ref _continuationState.PeekByRef();

                        if (outerState.FirstWrite == -1)
                            outerState.FirstWrite = arrayInfoStart;

                        if (outerState.State == ContinuationState.ReadPropertyName ||
                            outerState.State == ContinuationState.ReadPropertyValue)
                        {
                            ref var tagProperty = ref outerState.Properties.AddAsRef();
                            tagProperty.Type = (byte)_writeToken.WrittenToken;
                            tagProperty.Property = outerState.CurrentProperty;
                            tagProperty.Position = _writeToken.ValuePos;

                            outerState.State = ContinuationState.ReadPropertyName;
                        }
                        else if (outerState.State == ContinuationState.ReadArray)
                        {
                            outerState.Types.Add(_writeToken.WrittenToken);
                            outerState.Positions.Add(_writeToken.ValuePos);
                        }
                        else if (outerState.State == ContinuationState.ReadArrayDocument)
                        {
                            ref var tagArray = ref outerState.Properties.GetAsRef(0);
                            tagArray.Type = (byte)_writeToken.WrittenToken;
                            tagArray.Property = outerState.Properties[0].Property;
                            tagArray.Position = _writeToken.ValuePos;

                            // Register property position, name id (PropertyId) and type (object type and metadata)
                            _writeToken = _writer.WriteObjectMetadata(outerState.Properties, outerState.FirstWrite, outerState.MaxPropertyId);
                        }
                        else
                        {
                            ThrowIllegalStateException(outerState.State, "ReadEndArray");
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
            ref var currentState = ref _continuationState.PeekByRef();
            
            var valuePos = _writer.WriteNull();
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Null
            };

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
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Boolean
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(long value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Integer
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(float value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(ulong value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(double value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(decimal value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyNumberValue value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(string value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            BlittableJsonToken stringToken;
            var valuePos = _writer.WriteValue(value, out stringToken, _mode);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyStringValue value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            BlittableJsonToken stringToken;

            var valuePos = _writer.WriteValue(value, out stringToken, UsageMode.None, null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public void WriteValue(LazyCompressedStringValue value)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            BlittableJsonToken stringToken;

            var valuePos = _writer.WriteValue(value, out stringToken, UsageMode.None);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

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
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.RawBlob
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }

        public unsafe void WriteEmbeddedBlittableDocument(byte* ptr, int size)
        {
            ref var currentState = ref _continuationState.PeekByRef();
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.EmbeddedBlittable
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            FinishWritingScalarValue(ref currentState);
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void FinishWritingScalarValue(ref BuildingState currentState)
        {
            if (currentState.State == ContinuationState.ReadPropertyValue)
            {
                currentState.State = ContinuationState.ReadPropertyName;

                ref var tag = ref currentState.Properties.AddAsRef();
                tag.Position = _writeToken.ValuePos;
                tag.Type = (byte)_writeToken.WrittenToken;
                tag.Property = currentState.CurrentProperty;                
            }
            else if (currentState.State == ContinuationState.ReadArray)
            {
                currentState.Types.Add(_writeToken.WrittenToken);
                currentState.Positions.Add(_writeToken.ValuePos);
            }
            else
            {
                ThrowIllegalStateException(currentState.State, "ReadValue");
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
            BlittableJsonReaderArray array;
            if (reader.TryGet("_", out array))
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
