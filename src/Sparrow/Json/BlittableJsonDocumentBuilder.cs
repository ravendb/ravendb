using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Compression;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public class BlittableJsonDocumentBuilder : IDisposable
    {
        private readonly Stack<BuildingState> _continuationState = new Stack<BuildingState>();

        private readonly JsonOperationContext _context;
        private UsageMode _mode;
        private  IJsonParser _reader;
        private readonly JsonParserState _state;
        private UnmanagedWriteBuffer _unmanagedWriteBuffer;
        private AllocatedMemoryData _compressionBuffer;
        private int _position;
        private WriteToken _writeToken;
        private  string _debugTag;

        public int DiscardedCompressions, Compressed;

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, IJsonParser reader)
        {
            _context = context;
            _state = state;
            _reader = reader;
        }

        public void Reset(string debugTag, UsageMode mode)
        {
            _debugTag = debugTag;
            _mode = mode;
            _unmanagedWriteBuffer.Dispose();
            _unmanagedWriteBuffer = _context.GetStream();
            _position = 0;
            _continuationState.Clear();
            _writeToken = default(WriteToken);
            DiscardedCompressions = 0;
            Compressed = 0;
        }

        public void ReadArray()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadArrayDocument
            });
        }

        public void ReadObject()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadObjectDocument
            });
        }

        public void ReadNestedObject()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadObject
            });
        }

        public int SizeInBytes => _unmanagedWriteBuffer.SizeInBytes;


        public BlittableJsonDocumentBuilder(JsonOperationContext context, UsageMode mode, string debugTag, UnmanagedJsonParser jsonParser, JsonParserState state)
            :this(context, state, jsonParser)
        {
            Reset(debugTag, mode);
        }

        public void Dispose()
        {
            _unmanagedWriteBuffer.Dispose();
        }

        public bool Read()
        {
            if (_continuationState.Count == 0)
                return false; //nothing to do

            var currentState = _continuationState.Pop();
            while (true)
            {
                switch (currentState.State)
                {
                    case ContinuationState.ReadObjectDocument:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.ReadObject;
                        continue;
                    case ContinuationState.ReadArrayDocument:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }

                        var fakeFieldName = _context.GetLazyStringForFieldWithCaching("_");
                        var propIndex = _context.CachedProperties.GetPropertyId(fakeFieldName);
                        currentState.CurrentPropertyId = propIndex;
                        currentState.MaxPropertyId = propIndex;
                        currentState.FirstWrite = _position;
                        currentState.Properties = new List<PropertyTag>
                        {
                            new PropertyTag
                            {
                                PropertyId = propIndex
                            }
                        };
                        currentState.State = ContinuationState.CompleteDocumentArray;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadArray
                        };
                        continue;
                    case ContinuationState.CompleteDocumentArray:
                        currentState.Properties[0].Type = (byte)_writeToken.WrittenToken;
                        currentState.Properties[0].Position = _writeToken.ValuePos;

                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        _writeToken = FinalizeObjectWrite(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);

                        return true;
                    case ContinuationState.ReadObject:
                        if (_state.CurrentTokenType != JsonParserToken.StartObject)
                            throw new InvalidDataException("Expected start of object, but got " + _state.CurrentTokenType);
                        currentState.State = ContinuationState.ReadPropertyName;
                        currentState.Properties = new List<PropertyTag>();
                        currentState.FirstWrite = _position;
                        continue;
                    case ContinuationState.ReadArray:
                        if (_state.CurrentTokenType != JsonParserToken.StartArray)
                            throw new InvalidDataException("Expected start of array, but got " + _state.CurrentTokenType);
                        currentState.Types = new List<BlittableJsonToken>();
                        currentState.Positions = new List<int>();
                        currentState.State = ContinuationState.ReadArrayValue;
                        continue;
                    case ContinuationState.ReadArrayValue:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        {
                            currentState.State = ContinuationState.CompleteArray;
                            continue;
                        }
                        currentState.State = ContinuationState.CompleteArrayValue;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadValue
                        };
                        continue;
                    case ContinuationState.CompleteArrayValue:
                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        currentState.State = ContinuationState.ReadArrayValue;
                        continue;
                    case ContinuationState.CompleteArray:
                        var arrayInfoStart = _position;
                        var arrayToken = BlittableJsonToken.StartArray;

                        _position += WriteVariableSizeInt(currentState.Positions.Count);
                        if (currentState.Positions.Count == 0)
                        {
                            arrayToken |= BlittableJsonToken.OffsetSizeByte;
                            _writeToken = new WriteToken
                            {
                                ValuePos = arrayInfoStart,
                                WrittenToken = arrayToken
                            };
                        }
                        else
                        {
                            var distanceFromFirstItem = arrayInfoStart - currentState.Positions[0];
                            var distanceTypeSize = SetOffsetSizeFlag(ref arrayToken, distanceFromFirstItem);

                            for (var i = 0; i < currentState.Positions.Count; i++)
                            {
                                WriteNumber(arrayInfoStart - currentState.Positions[i], distanceTypeSize);
                                _position += distanceTypeSize;

                                _unmanagedWriteBuffer.WriteByte((byte)currentState.Types[i]);
                                _position++;
                            }

                            _writeToken = new WriteToken
                            {
                                ValuePos = arrayInfoStart,
                                WrittenToken = arrayToken
                            };
                        }
                        currentState = _continuationState.Pop();
                        continue;
                    case ContinuationState.ReadPropertyName:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }

                        if (_state.CurrentTokenType == JsonParserToken.EndObject)
                        {
                            _writeToken = FinalizeObjectWrite(currentState.Properties, currentState.FirstWrite,
                                currentState.MaxPropertyId);
                            if (_continuationState.Count == 0)
                                return true;
                            currentState = _continuationState.Pop();
                            continue;
                        }

                        if (_state.CurrentTokenType != JsonParserToken.String)
                            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType);


                        var property = CreateLazyStringValueFromParserState();
                        if (_state.EscapePositions.Count > 0)
                        {
                            property.EscapePositions = _state.EscapePositions.ToArray();
                        }

                        currentState.CurrentPropertyId = _context.CachedProperties.GetPropertyId(property);
                        currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentPropertyId);
                        currentState.State = ContinuationState.ReadPropertyValue;
                        continue;
                    case ContinuationState.ReadPropertyValue:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.CompleteReadingPropertyValue;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadValue
                        };
                        continue;
                    case ContinuationState.CompleteReadingPropertyValue:
                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        currentState.Properties.Add(new PropertyTag
                        {
                            Position = _writeToken.ValuePos,
                            Type = (byte)_writeToken.WrittenToken,
                            PropertyId = currentState.CurrentPropertyId
                        });
                        currentState.State = ContinuationState.ReadPropertyName;
                        continue;
                    case ContinuationState.ReadValue:
                        ReadJsonValue();
                        currentState = _continuationState.Pop();
                        break;
                }
            }
        }

        private void ReadJsonValue()
        {
            var start = _position;
            switch (_state.CurrentTokenType)
            {
                case JsonParserToken.StartObject:
                    _continuationState.Push(new BuildingState
                    {
                        State = ContinuationState.ReadObject
                    });
                    return;
                case JsonParserToken.StartArray:
                    _continuationState.Push(new BuildingState
                    {
                        State = ContinuationState.ReadArray
                    });
                    return;
                case JsonParserToken.Integer:
                    _position += WriteVariableSizeLong(_state.Long);
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Integer
                    };
                    return;
                case JsonParserToken.Float:
                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                        _reader.ValidateFloat();
                    BlittableJsonToken ignored;
                    WriteStringFromReader(out ignored);
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Float
                    };
                    return;
                case JsonParserToken.String:
                    BlittableJsonToken stringToken;
                    WriteStringFromReader(out stringToken);
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = stringToken
                    };
                    return;
                case JsonParserToken.True:
                case JsonParserToken.False:
                    _unmanagedWriteBuffer.WriteByte(_state.CurrentTokenType == JsonParserToken.True ? (byte)1 : (byte)0);
                    _position++;
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Boolean
                    };
                    return;
                case JsonParserToken.Null:
                    _unmanagedWriteBuffer.WriteByte(0);
                    _position++;
                    _writeToken = new WriteToken // nothing to do here, we handle that with the token
                    {
                        WrittenToken = BlittableJsonToken.Null,
                        ValuePos = start
                    };
                    return;
                default:
                    throw new InvalidDataException("Expected a value, but got " + _state.CurrentTokenType);
            }
        }


        private enum ContinuationState
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

        private struct BuildingState
        {
            public ContinuationState State;
            public List<PropertyTag> Properties;
            public int CurrentPropertyId;
            public int MaxPropertyId;
            public List<BlittableJsonToken> Types;
            public List<int> Positions;
            public int FirstWrite;
        }


        public class PropertyTag
        {
            public int Position;
            public int PropertyId;
            public byte Type;
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
        }


        private unsafe LazyStringValue CreateLazyStringValueFromParserState()
        {
            return new LazyStringValue(null, _state.StringBuffer, _state.StringSize, _context);
        }


        private int WritePropertyNames(int rootOffset)
        {
            // Write the property names and register their positions
            var propertyArrayOffset = new int[_context.CachedProperties.PropertiesDiscovered];
            for (var index = 0; index < propertyArrayOffset.Length; index++)
            {
                propertyArrayOffset[index] = WritePropertyString(_context.CachedProperties.GetProperty(index));
            }

            // Register the position of the properties offsets start
            var propertiesStart = _position;

            // Find the minimal space to store the offsets (byte,short,int) and raise the appropriate flag in the properties metadata
            BlittableJsonToken propertiesSizeMetadata = 0;
            var propertyNamesOffset = _position - rootOffset;
            var propertyArrayOffsetValueByteSize = SetOffsetSizeFlag(ref propertiesSizeMetadata, propertyNamesOffset);

            WriteNumber((int)propertiesSizeMetadata, sizeof(byte));

            // Write property names offsets
            foreach (int offset in propertyArrayOffset)
            {
                WriteNumber(propertiesStart - offset, propertyArrayOffsetValueByteSize);
            }
            return propertiesStart;
        }


        private static int SetOffsetSizeFlag(ref BlittableJsonToken objectToken, int distanceFromFirstProperty)
        {
            int positionSize;
            if (distanceFromFirstProperty <= byte.MaxValue)
            {
                positionSize = sizeof(byte);
                objectToken |= BlittableJsonToken.OffsetSizeByte;
            }
            else
            {
                if (distanceFromFirstProperty <= ushort.MaxValue)
                {
                    positionSize = sizeof(short);
                    objectToken |= BlittableJsonToken.OffsetSizeShort;
                }
                else
                {
                    positionSize = sizeof(int);
                    objectToken |= BlittableJsonToken.OffsetSizeInt;
                }
            }
            return positionSize;
        }

        private int WritePropertyString(LazyStringValue prop)
        {
            BlittableJsonToken token;
            var startPos = WriteString(prop, out token, UsageMode.None);
            if (prop.EscapePositions == null)
            {
                _position += WriteVariableSizeInt(0);
                return startPos;
            }
            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(prop.EscapePositions.Length);
            foreach (int escapePos in prop.EscapePositions)
            {
                _position += WriteVariableSizeInt(escapePos);
            }
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            switch (sizeOfValue)
            {
                case sizeof(int):
                    _unmanagedWriteBuffer.WriteByte((byte)value);
                    _unmanagedWriteBuffer.WriteByte((byte)(value >> 8));
                    _unmanagedWriteBuffer.WriteByte((byte)(value >> 16));
                    _unmanagedWriteBuffer.WriteByte((byte)(value >> 24));
                    break;
                case sizeof(short):
                    _unmanagedWriteBuffer.WriteByte((byte)value);
                    _unmanagedWriteBuffer.WriteByte((byte)(value >> 8));
                    break;
                case sizeof(byte):
                    _unmanagedWriteBuffer.WriteByte((byte)value);
                    break;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        public unsafe int WriteVariableSizeLong(long value)
        {
            // see zig zap trick here:
            // https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
            // for negative values

            var buffer = stackalloc byte[10];
            var count = 0;
            var v = (ulong)((value << 1) ^ (value >> 63));
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);
            _unmanagedWriteBuffer.Write(buffer, count);
            return count;
        }

        public unsafe int WriteVariableSizeInt(int value)
        {
            // assume that we don't use negative values very often
            var buffer = stackalloc byte[5];
            var count = 0;
            var v = (uint)value;
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);
            _unmanagedWriteBuffer.Write(buffer, count);
            return count;
        }

        public unsafe int WriteVariableSizeIntInReverse(int value)
        {
            // assume that we don't use negative values very often
            var buffer = stackalloc byte[5];
            var count = 0;
            var v = (uint)value;
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);
            for (int i = count - 1; i >= count / 2; i--)
            {
                var tmp = buffer[i];
                buffer[i] = buffer[count - 1 - i];
                buffer[count - 1 - i] = tmp;
            }
            _unmanagedWriteBuffer.Write(buffer, count);
            return count;
        }

        public void FinalizeDocument()
        {
            var token = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            var propertiesStart = WritePropertyNames(rootOffset);

            WriteVariableSizeIntInReverse(rootOffset);
            WriteVariableSizeIntInReverse(propertiesStart);
            WriteNumber((int)token, sizeof(byte));
        }

        public unsafe int WriteString(LazyStringValue str, out BlittableJsonToken token, UsageMode state)
        {
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(str.Size);
            var buffer = str.Buffer;
            var size = str.Size;
            var maxGoodCompressionSize =
                     // if we are more than this size, we want to abort the compression early and just use
                     // the verbatim string
                     str.Size - sizeof(int) * 2;
            var shouldCompress =
                _state.CompressedSize != null ||
                (((state & UsageMode.CompressStrings) == UsageMode.CompressStrings) && (size > 128))
                || ((state & UsageMode.CompressSmallStrings) == UsageMode.CompressSmallStrings) && (size <= 128);
            if (maxGoodCompressionSize > 0 && shouldCompress)
            {
                Compressed++;


                int compressedSize;
                byte* compressionBuffer;
                if (_state.CompressedSize != null)
                {
                    // we already have compressed data here
                    compressedSize = _state.CompressedSize.Value;
                    compressionBuffer = _state.StringBuffer;
                    _state.CompressedSize = null;
                }
                else
                {
                    compressionBuffer = CompressBuffer(str, maxGoodCompressionSize, out compressedSize);
                }
                if (compressedSize > 0)// only if we actually save more than space
                {
                    token = BlittableJsonToken.CompressedString;
                    buffer = compressionBuffer;
                    size = compressedSize;
                    _position += WriteVariableSizeInt(compressedSize);
                }
                else
                {
                    DiscardedCompressions++;
                }
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;
            return startPos;
        }

        private unsafe byte* CompressBuffer(LazyStringValue str, int maxGoodCompressionSize, out int compressedSize)
        {
            var compressionBuffer = GetCompressionBuffer(str.Size);
            if (str.Size > 128)
            {
                compressedSize = _context.Lz4.Encode64(str.Buffer,
                    compressionBuffer,
                    str.Size,
                    maxGoodCompressionSize,
                    acceleration: CalculateCompressionAcceleration(str.Size));
            }
            else
            {
                compressedSize = SmallStringCompression.Instance.Compress(str.Buffer,
                    compressionBuffer,
                    str.Size,
                    maxGoodCompressionSize);
            }
            return compressionBuffer;
        }

        private static int CalculateCompressionAcceleration(int size)
        {
            return (int)Math.Log(size, 2);
        }


        private unsafe byte* GetCompressionBuffer(int minSize)
        {
            // enlarge buffer if needed
            if (_compressionBuffer == null ||
                minSize > _compressionBuffer.SizeInBytes)
            {
                _compressionBuffer = _context.GetMemory(minSize);
            }
            return (byte*)_compressionBuffer.Address;
        }
        private unsafe void WriteStringFromReader(out BlittableJsonToken token)
        {
            var str = new LazyStringValue(null, _state.StringBuffer, _state.StringSize, _context);
            WriteString(str, out token, _mode);

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(_state.EscapePositions.Count);
            foreach (int escapePos in _state.EscapePositions)
            {
                _position += WriteVariableSizeInt(escapePos);
            }
        }

        private WriteToken FinalizeObjectWrite(List<PropertyTag> properties, int firstWrite, int maxPropId)
        {
            _context.CachedProperties.Sort(properties);

            var objectMetadataStart = _position;
            var distanceFromFirstProperty = objectMetadataStart - firstWrite;

            // Find metadata size and properties offset and set appropriate flags in the BlittableJsonToken
            var objectToken = BlittableJsonToken.StartObject;
            var positionSize = SetOffsetSizeFlag(ref objectToken, distanceFromFirstProperty);
            var propertyIdSize = SetPropertyIdSizeFlag(ref objectToken, maxPropId);

            _position += WriteVariableSizeInt(properties.Count);

            // Write object metadata
            foreach (var sortedProperty in properties)
            {
                WriteNumber(objectMetadataStart - sortedProperty.Position, positionSize);
                WriteNumber(sortedProperty.PropertyId, propertyIdSize);
                _unmanagedWriteBuffer.WriteByte(sortedProperty.Type);
                _position += positionSize + propertyIdSize + sizeof(byte);
            }

            return new WriteToken
            {
                ValuePos = objectMetadataStart,
                WrittenToken = objectToken
            };
        }


        private static int SetPropertyIdSizeFlag(ref BlittableJsonToken objectToken, int maxPropId)
        {
            int propertyIdSize;
            if (maxPropId <= byte.MaxValue)
            {
                propertyIdSize = sizeof(byte);
                objectToken |= BlittableJsonToken.PropertyIdSizeByte;
            }
            else
            {
                if (maxPropId <= ushort.MaxValue)
                {
                    propertyIdSize = sizeof(short);
                    objectToken |= BlittableJsonToken.PropertyIdSizeShort;
                }
                else
                {
                    propertyIdSize = sizeof(int);
                    objectToken |= BlittableJsonToken.PropertyIdSizeInt;
                }
            }
            return propertyIdSize;
        }


        public unsafe BlittableJsonReaderObject CreateReader()
        {
            byte* ptr;
            int size;
            _unmanagedWriteBuffer.EnsureSingleChunk(out ptr, out size);
            var reader = new BlittableJsonReaderObject(ptr, size, _context, _unmanagedWriteBuffer);
            _unmanagedWriteBuffer = default(UnmanagedWriteBuffer);
            return reader;
        }

        public BlittableJsonReaderArray CreateArrayReader()
        {
            var reader = CreateReader();
            BlittableJsonReaderArray array;
            if (reader.TryGet("_", out array))
                return array;
            throw new InvalidOperationException("Couldn't find array");
        }


        public override string ToString()
        {
            return "Building json for " + _debugTag;
        }

        public unsafe void CopyTo(IntPtr ptr)
        {
            _unmanagedWriteBuffer.CopyTo((byte*)ptr);
        }


        public unsafe void CopyTo(MemoryStream stream)
        {
            stream.SetLength(stream.Position + SizeInBytes);
            ArraySegment<byte> bytes;
            stream.TryGetBuffer(out bytes);
            fixed (byte* ptr = bytes.Array)
            {
                _unmanagedWriteBuffer.CopyTo(ptr + stream.Position);
                stream.Position += SizeInBytes;
            }
        }
    }
}