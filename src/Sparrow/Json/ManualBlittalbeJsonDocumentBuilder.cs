using System;
using System.Collections.Generic;
using System.IO;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public class ManualBlittalbeJsonDocumentBuilder<TWriter> : IDisposable
        where TWriter : struct, IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableWriter<TWriter> _writer;
        private readonly Stack<BuildingState> _continuationState = new Stack<BuildingState>();
        private UsageMode _mode;
        private WriteToken _writeToken;


        /// <summary>
        /// Allows incrementally building json document
        /// </summary>
        /// <param name="context"></param>
        /// <param name="mode"></param>
        /// <param name="writer"></param>
        public ManualBlittalbeJsonDocumentBuilder(
            JsonOperationContext context,
            UsageMode? mode = null,
            BlittableWriter<TWriter> writer = null)
        {
            _context = context;
            _mode = mode ?? UsageMode.None;
            _writer = writer ?? new BlittableWriter<TWriter>(_context);
        }

        public virtual void StartWriteObjectDocument()
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
                State = ContinuationState.ReadArrayDocument
            };

            var fakeFieldName = _context.GetLazyStringForFieldWithCaching("_");
            var propIndex = _context.CachedProperties.GetPropertyId(fakeFieldName);
            currentState.CurrentPropertyId = propIndex;
            currentState.MaxPropertyId = propIndex;
            currentState.FirstWrite = _writer.Position;
            currentState.Properties = new List<PropertyTag>
                        {
                            new PropertyTag
                            {
                                PropertyId = propIndex
                            }
                        };

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


            int newPropertyId = _context.CachedProperties.GetPropertyId(property);
            currentState.CurrentPropertyId = newPropertyId;
            currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentPropertyId);
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
                Properties = new List<PropertyTag>(),
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

                        // here we know that the last itme in the stack is the keep the last ReadObjectDocument
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
                                outerState.Properties.Add(new PropertyTag
                                {
                                    Position = _writeToken.ValuePos,
                                    Type = (byte)_writeToken.WrittenToken,
                                    PropertyId = outerState.CurrentPropertyId
                                });
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
                            PropertyId = currentState.CurrentPropertyId
                        });
                        if (currentState.FirstWrite == -1)
                            currentState.FirstWrite = start;

                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite,
                                            currentState.MaxPropertyId);
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
                Types = new List<BlittableJsonToken>(),
                Positions = new List<int>()
            });
        }

        public void WriteArrayEnd()
        {
            var currentState = _continuationState.Pop();

            switch (currentState.State)
            {
                case ContinuationState.ReadArrayDocument:
                    currentState.Properties[0].Type = (byte)_writeToken.WrittenToken;
                    currentState.Properties[0].Position = _writeToken.ValuePos;

                    // Register property position, name id (PropertyId) and type (object type and metadata)
                    _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                    _continuationState.Push(currentState);
                    break;
                case ContinuationState.ReadArray:
                    var arrayToken = BlittableJsonToken.StartArray;
                    var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types,
                        ref arrayToken);

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
                            outerState.Properties.Add(new PropertyTag
                            {
                                Position = _writeToken.ValuePos,
                                Type = (byte)_writeToken.WrittenToken,
                                PropertyId = outerState.CurrentPropertyId
                            });
                            outerState.State = ContinuationState.ReadPropertyName;
                        }
                        else if (outerState.State == ContinuationState.ReadArray)
                        {
                            outerState.Types.Add(_writeToken.WrittenToken);
                            outerState.Positions.Add(_writeToken.ValuePos);
                        }
                        else if (outerState.State == ContinuationState.ReadArrayDocument)
                        {
                            outerState.Properties[0].Type = (byte)_writeToken.WrittenToken;
                            outerState.Properties[0].Position = _writeToken.ValuePos;

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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Null
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(bool value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Float
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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Float
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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Float
            };

            if (currentState.FirstWrite == -1)
                currentState.FirstWrite = valuePos;

            currentState = FinishWritingScalarValue(currentState);
            _continuationState.Push(currentState);
        }

        public void WriteValue(LazyDoubleValue value)
        {
            var currentState = _continuationState.Pop();
            var valuePos = _writer.WriteValue(value);
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
            {
                ValuePos = valuePos,
                WrittenToken = BlittableJsonToken.Float
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
            _writeToken = new WriteToken //todo: figure out if we really need those WriteTokens
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
            var currentState = _continuationState.Pop();
            BlittableJsonToken token;
            var valuePos = _writer.WriteValue(document.BasePointer, document.Size, out token, UsageMode.None, null);
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
                    currentState.Properties.Add(new BlittableJsonDocumentBuilder.PropertyTag
                    {
                        Position = _writeToken.ValuePos,
                        Type = (byte)_writeToken.WrittenToken,
                        PropertyId = currentState.CurrentPropertyId
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

        //todo: consider allowing more forgiving functionality that will pop all states and close relevant objects/arrays
        public void FinalizeDocument()
        {
            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);
        }

        private void ThrowIllegalStateException(BlittableJsonDocumentBuilder.ContinuationState state, string realOperation)
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
        }
    }
}