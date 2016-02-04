using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Server.Json.Parsing;

namespace Raven.Server.Json
{

    public class BlittableJsonDocumentBuilder : IDisposable
    {
        private readonly RavenOperationContext _context;
        private readonly UsageMode _mode;
        private readonly JsonParserState _state;
        private readonly IJsonParser _reader;
        private readonly UnmanagedWriteBuffer _stream;
        private UnmanagedBuffersPool.AllocatedMemoryData _buffer, _compressionBuffer;

        private int _position;
        public int DiscardedCompressions, Compressed;

        [Flags]
        public enum UsageMode
        {
            None = 0,
            ValidateDouble = 1,
            CompressStrings = 2,
            CompressSmallStrings = 4,
            ToDisk = ValidateDouble | CompressStrings |  CompressSmallStrings
        }

        internal BlittableJsonDocumentBuilder(RavenOperationContext context, UsageMode mode, string documentId, IJsonParser reader, JsonParserState state)
        {
            _reader = reader;
            _stream = context.GetStream(documentId);
            _context = context;
            _mode = mode;
            _state = state;
        }

        public int SizeInBytes => _stream.SizeInBytes;

        public void Dispose()
        {
            if (_buffer != null)
            {
                _context.ReturnMemory(_buffer);
                _buffer = null;
            }
            if (_compressionBuffer != null)
            {
                _context.ReturnMemory(_compressionBuffer);
                _compressionBuffer = null;
            }

            _stream.Dispose();
        }

        private unsafe byte* GetCompressionBuffer(int minSize)
        {
            // enlarge buffer if needed
            if (_compressionBuffer == null ||
                minSize > _compressionBuffer.SizeInBytes)
            {
                if (_compressionBuffer != null)
                    _context.ReturnMemory(_compressionBuffer);

                _compressionBuffer = _context.GetMemory(minSize);
            }
            return (byte*)_compressionBuffer.Address;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe byte* GetTempBuffer(int minSize)
        {
            if (_buffer != null && minSize <= _buffer.SizeInBytes)
                return (byte*)_buffer.Address;
            if (_buffer != null)
                _context.ReturnMemory(_buffer);
            _buffer = _context.GetMemory(minSize);
            return (byte*)_buffer.Address;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int CopyTo(byte* ptr)
        {
            return _stream.CopyTo(ptr);
        }

        public unsafe BlittableJsonReaderObject CreateReader()
        {
            byte* ptr;
            int size;
            _stream.EnsureSingleChunk(out ptr, out size);
            return new BlittableJsonReaderObject(ptr, size, _context, this);
        }

        /// <summary>
        /// Writes the json object from  reader received in the ctor into the received UnmanangedWriteBuffer
        /// </summary>
        public async Task Run()
        {
            await _reader.ReadAsync();
            if (_state.CurrentTokenType != JsonParserToken.StartObject)
                throw new InvalidDataException("Expected start of object, but got " + _state.CurrentTokenType);
            BlittableJsonToken token;

            // Write the whole object recursively
            var writeToken = await WriteObject();
            token = writeToken.WrittenToken;
            var rootOffset = writeToken.ValuePos;

            // Write the property names and register it's positions
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
            for (var i = 0; i < propertyArrayOffset.Length; i++)
            {
                WriteNumber(propertiesStart - propertyArrayOffset[i], propertyArrayOffsetValueByteSize);
            }
            WriteNumber(rootOffset, sizeof(int));
            WriteNumber(propertiesStart, sizeof(int));
            WriteNumber((int)token, sizeof(byte));
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
            if (prop.EscapePositions.Length > 0)
            {
                for (int i = 0; i < prop.EscapePositions.Length; i++)
                {
                    _position += WriteVariableSizeInt(prop.EscapePositions[i] );
                }
            }
            return startPos;
        }

        public struct WriteToken
        {
            public int ValuePos;
            public BlittableJsonToken WrittenToken;
        }

        private unsafe LazyStringValue CerateLazyStringValueByState()
        {
            return new LazyStringValue(null,_state.StringBuffer, _state.StringSize, _context);
        }
        /// <summary>
        /// Write an object to the UnmangedBuffer
        /// </summary>
        /// <param name="objectToken"></param>
        /// <returns></returns>
        private async Task<WriteToken> WriteObject()
        {
            BlittableJsonToken objectToken;
            var properties = new List<PropertyTag>();
            var firstWrite = _position;
            var maxPropId = -1;

            // Iterate through the object's properties, write it to the UnmanagedWriteBuffer and register it's names and positions
            while (true)
            {
                await _reader.ReadAsync();

                if (_state.CurrentTokenType == JsonParserToken.EndObject)
                    break;

                if (_state.CurrentTokenType != JsonParserToken.String)
                    throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType);


                var property = CerateLazyStringValueByState();
                if (_state.EscapePositions.Count > 0)
                {
                    property.EscapePositions = _state.EscapePositions.ToArray();
                }
                
                var propIndex = _context.CachedProperties.GetPropertyId(property);

                maxPropId = Math.Max(maxPropId, propIndex);

                await _reader.ReadAsync();

                // Write object property into the UnmanagedWriteBuffer
                var writeToken = await WriteValue();

                // Register property position, name id (PropertyId) and type (object type and metadata)
                properties.Add(new PropertyTag
                {
                    Position = writeToken.ValuePos,
                    Type = (byte)writeToken.WrittenToken,
                    PropertyId = propIndex
                });
            }

            _context.CachedProperties.Sort(properties);

            var objectMetadataStart = _position;
            var distanceFromFirstProperty = objectMetadataStart - firstWrite;

            // Find metadata size and properties offset and set appropriate flags in the BlittableJsonToken
            objectToken = BlittableJsonToken.StartObject;
            var positionSize = SetOffsetSizeFlag(ref objectToken, distanceFromFirstProperty);
            var propertyIdSize = SetPropertyIdSizeFlag(ref objectToken, maxPropId);

            _position += WriteVariableSizeInt(properties.Count);

            // Write object metadata
            foreach (var sortedProperty in properties)
            {
                WriteNumber(objectMetadataStart - sortedProperty.Position, positionSize);
                WriteNumber(sortedProperty.PropertyId, propertyIdSize);
                _stream.WriteByte(sortedProperty.Type);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task<WriteToken> WriteValue()
        {
            var start = _position;
            switch (_state.CurrentTokenType)
            {
                case JsonParserToken.StartObject:
                    return await WriteObject();
                case JsonParserToken.StartArray:
                    return await WriteArray();
                case JsonParserToken.Integer:
                    _position += WriteVariableSizeLong(_state.Long);                   
                    return new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Integer
                    };
                case JsonParserToken.Float:
                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                        _reader.ValidateFloat();
                    BlittableJsonToken ignored;
                    WriteStringFromReader(out ignored);
                    return new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Float
                    };
                case JsonParserToken.String:
                    BlittableJsonToken stringToken;
                    WriteStringFromReader(out stringToken);
                    return new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = stringToken
                    };
                case JsonParserToken.True:
                case JsonParserToken.False:
                    _stream.WriteByte(_state.CurrentTokenType == JsonParserToken.True ? (byte)1 : (byte)0);
                    _position++;
                    return new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Boolean
                    };
                case JsonParserToken.Null:
                    _stream.WriteByte(0);
                    _position++;
                    return new WriteToken // nothing to do here, we handle that with the token
                    {
                        WrittenToken = BlittableJsonToken.Null,
                        ValuePos = start
                    };
                default:
                    throw new InvalidDataException("Expected a value, but got " + _state.CurrentTokenType);
                    // ReSharper restore RedundantCaseLabel
            }
        }

        private unsafe void WriteStringFromReader(out BlittableJsonToken token)
        {
            var str = new LazyStringValue(null, _state.StringBuffer, _state.StringSize, _context);
            WriteString(str, out token, _mode);
            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(_state.EscapePositions.Count);
            if (_state.EscapePositions.Count > 0)
            {
                for (int i = 0; i < _state.EscapePositions.Count; i++)
                {
                    _position += WriteVariableSizeInt(_state.EscapePositions[i]);
                }
            }
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }
        private async Task<WriteToken> WriteArray()
        {
            BlittableJsonToken arrayToken;
            var positions = new List<int>();
            var types = new List<BlittableJsonToken>();
            while (true)
            {
                await _reader.ReadAsync();
                if (_state.CurrentTokenType == JsonParserToken.EndArray)
                    break;
                
                var writeToken = await WriteValue();
                types.Add(writeToken.WrittenToken);
                positions.Add(writeToken.ValuePos);
            }
            var arrayInfoStart = _position;
            arrayToken = BlittableJsonToken.StartArray;

            _position += WriteVariableSizeInt(positions.Count);
            if (positions.Count == 0)
            {
                arrayToken |= BlittableJsonToken.OffsetSizeByte;
                return new WriteToken
                {
                    ValuePos = arrayInfoStart,
                    WrittenToken = arrayToken
                };

            }

            var distanceFromFirstItem = arrayInfoStart - positions[0];
            var distanceTypeSize = SetOffsetSizeFlag(ref arrayToken, distanceFromFirstItem);

            for (var i = 0; i < positions.Count; i++)
            {
                WriteNumber(arrayInfoStart - positions[i], distanceTypeSize);
                _position += distanceTypeSize;

                _stream.WriteByte((byte)types[i]);
                _position++;
            }

            return new WriteToken
            {
                ValuePos = arrayInfoStart,
                WrittenToken = arrayToken
            };
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
                ((state & UsageMode.CompressStrings) == UsageMode.CompressStrings && size > 128) ||
                (state & UsageMode.CompressSmallStrings) == UsageMode.CompressSmallStrings;
            if (maxGoodCompressionSize > 0  && shouldCompress)
            {
                Compressed++;


                int compressedSize;
                byte* compressionBuffer;
                if (_state.CompressedSize != null)
                {
                    // we already have compressed data here
                    compressedSize = _state.CompressedSize.Value;
                    compressionBuffer = _state.StringBuffer;
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

            _stream.Write(buffer, size);
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
                    maxGoodCompressionSize);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            switch (sizeOfValue)
            {
                case sizeof(int):
                    _stream.WriteByte((byte)value);
                    _stream.WriteByte((byte)(value >> 8));
                    _stream.WriteByte((byte)(value >> 16));
                    _stream.WriteByte((byte)(value >> 24));
                    break;
                case sizeof(short):
                    _stream.WriteByte((byte)value);
                    _stream.WriteByte((byte)(value >> 8));
                    break;
                case sizeof(byte):
                    _stream.WriteByte((byte)value);
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
            _stream.Write(buffer, count);
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
            _stream.Write(buffer, count);
            return count;
        }

        public class PropertyTag
        {
            public int Position;
            public int PropertyId;
            public byte Type;
        }

    }
}