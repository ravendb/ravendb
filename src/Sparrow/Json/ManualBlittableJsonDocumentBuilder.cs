using System;
using System.Collections.Generic;
using Sparrow.Collections;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public sealed class ManualBlittableJsonDocumentBuilder<TWriter> : BlittableJsonDocumentBuilderCache, IDisposable
        where TWriter : struct, IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableWriter<TWriter> _writer;
        private readonly Stack<BuildingState> _continuationState = new Stack<BuildingState>();
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
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadObjectDocument
            });
        }
        public void StartArrayDocument()
        {
            var currentState = new BuildingState
            {
                State = ContinuationState.ReadArrayDocument,
            };

            var fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
            var prop = _writer.CachedProperties.GetProperty(fakeFieldName);
            currentState.CurrentProperty = prop;
            currentState.MaxPropertyId = prop.PropertyId;
            currentState.FirstWrite = _writer.Position;
            currentState.Properties = _propertiesCache.Allocate();
            currentState.Properties.Add(new PropertyTag {Property = prop});
            _continuationState.Push(currentState);
        }

        public void WritePropertyName(string propertyName)
        {
            var property = _context.GetLazyStringForFieldWithCaching(propertyName);
            WritePropertyName(property);
        }

        public void WritePropertyName(LazyStringValue property)
        {
            var currentState = _continuationState.Pop();

            if (currentState.State != ContinuationState.ReadPropertyName)
            {
                ThrowIllegalStateException(currentState.State, "WritePropertyName");
            }

            var newPropertyId = _writer.CachedProperties.GetProperty(property);
            currentState.CurrentProperty = newPropertyId;
            currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
            currentState.State = ContinuationState.ReadPropertyValue;
            _continuationState.Push(currentState);
        }

        public void StartWriteObject()
        {
            var previousState = _continuationState.Pop();

            if (previousState.State != ContinuationState.ReadObjectDocument &&
                previousState.State != ContinuationState.ReadPropertyValue &&
                previousState.State != ContinuationState.ReadArray)
                ThrowIllegalStateException(previousState.State, "WriteObject");

            previousState.State = previousState.State == ContinuationState.ReadPropertyValue ? ContinuationState.ReadPropertyName : previousState.State;
            _continuationState.Push(previousState);

            _continuationState.Push(new BuildingState()
            {
                State = ContinuationState.ReadPropertyName,
                Properties = _propertiesCache.Allocate(),
                FirstWrite = -1
            });
        }

        public void WriteObjectEnd()
        {
            var currentState = _continuationState.Pop();
            long start = 0;
            start = _writer.Position;
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
                            var outerState = _continuationState.Pop();
                            if (outerState.State == ContinuationState.ReadArray)
                            {
                                outerState.Types.Add(_writeToken.WrittenToken);
                                outerState.Positions.Add(_writeToken.ValuePos);
                            }
                            else
                            {
                                outerState.Properties.Add(new PropertyTag(
                                    type: (byte)_writeToken.WrittenToken,
                                    property: outerState.CurrentProperty,
                                    position: _writeToken.ValuePos                                    
                                ));
                            }

                            if (outerState.FirstWrite == -1)
                                outerState.FirstWrite = start;
                            _continuationState.Push(outerState);
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
                        currentState.Properties.Add(new PropertyTag
                        {
                            Position = _writeToken.ValuePos,
                            Type = (byte)_writeToken.WrittenToken,
                            Property = currentState.CurrentProperty
                        });
                        if (currentState.FirstWrite == -1)
                            currentState.FirstWrite = start;

                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite,
                                            currentState.MaxPropertyId);
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
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadArray,
                Types = _tokensCache.Allocate(),
                Positions = _positionsCache.Allocate()
            });
        }

        public void WriteArrayEnd()
        {
            var currentState = _continuationState.Pop();

            switch (currentState.State)
            {
                case ContinuationState.ReadArrayDocument:
                    currentState.Properties[0] = new PropertyTag
                    {
                        Property = currentState.Properties[0].Property,
                        Type = (byte) _writeToken.WrittenToken,
                        Position = _writeToken.ValuePos
                    };

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
                        var outerState = _continuationState.Pop();

                        if (outerState.FirstWrite == -1)
                            outerState.FirstWrite = arrayInfoStart;

                        if (outerState.State == ContinuationState.ReadPropertyName ||
                            outerState.State == ContinuationState.ReadPropertyValue)
                        {
                            outerState.Properties.Add(new PropertyTag(
                                type: (byte)_writeToken.WrittenToken,
                                property: outerState.CurrentProperty,
                                position: _writeToken.ValuePos
                            ));
                            outerState.State = ContinuationState.ReadPropertyName;
                        }
                        else if (outerState.State == ContinuationState.ReadArray)
                        {
                            outerState.Types.Add(_writeToken.WrittenToken);
                            outerState.Positions.Add(_writeToken.ValuePos);
                        }
                        else if (outerState.State == ContinuationState.ReadArrayDocument)
                        {
                            outerState.Properties[0] = new PropertyTag(
                                type: (byte)_writeToken.WrittenToken,
                                property: outerState.Properties[0].Property,                             
                                position: _writeToken.ValuePos
                            );

                            // Register property position, name id (PropertyId) and type (object type and metadata)
                            _writeToken = _writer.WriteObjectMetadata(outerState.Properties, outerState.FirstWrite, outerState.MaxPropertyId);
                        }
                        else
                        {
                            ThrowIllegalStateException(outerState.State, "ReadEndArray");
                        }

                        _continuationState.Push(outerState);

                    }

                    break;
                default:
                    ThrowIllegalStateException(currentState.State, "ReadEndArray");
                    break;
            }
        }

        public void WriteValueNull()
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteNull();
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Null
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
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
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Boolean
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(long value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Integer
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(float value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(ulong value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(double value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(decimal value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(LazyNumberValue value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.LazyNumber
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(string value)
        {
            var currentState = _continuationState.Pop();
            BlittableJsonToken stringToken;
            var valuePos = _writer.WriteValue(value, out stringToken, _mode);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(LazyStringValue value)
        {
            var currentState = _continuationState.Pop();
            BlittableJsonToken stringToken;
            

            var valuePos = _writer.WriteValue(value, out stringToken,UsageMode.None,null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(LazyCompressedStringValue value)
        {
            var currentState = _continuationState.Pop();
            BlittableJsonToken stringToken;

            //public unsafe int WriteValue(byte* buffer, int size, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
            //var valuePos = _writer.WriteValue(value, out stringToken, UsageMode.None, null);
            var valuePos = _writer.WriteValue(value, out stringToken, UsageMode.None);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = stringToken
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public unsafe void WriteEmbeddedBlittableDocument(BlittableJsonReaderObject document)
        {
            WriteEmbeddedBlittableDocument(document.BasePointer, document.Size);
        }

        public unsafe void WriteRawBlob(byte* ptr, int size)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.RawBlob
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public unsafe void WriteEmbeddedBlittableDocument(byte* ptr, int size)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(ptr, size, out _, UsageMode.None, null);
            _writeToken = new WriteToken
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.EmbeddedBlittable
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        private BuildingState FinishWritingScalarValue(BuildingState currentState)
        {
            switch (currentState.State)
            {
                case ContinuationState.ReadPropertyValue:
                    currentState.Properties.Add(new PropertyTag
                    {
                        Position = _writeToken.ValuePos,
                        Type = (byte)_writeToken.WrittenToken,
                        Property = currentState.CurrentProperty
                    });

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
            return currentState;
        }

        public void FinalizeDocument()
        {
            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);

            while (_continuationState.Count > 0)
            {
                var state = _continuationState.Pop();
                _propertiesCache.Return(ref state.Properties);
                _tokensCache.Return(ref state.Types);
                _positionsCache.Return(ref state.Positions);
            }
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

        public void Dispose()
        {
            _writer.Dispose();
            base.Dispose();
        }
    }
}
