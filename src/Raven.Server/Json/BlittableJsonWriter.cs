using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Voron.Util;

//using Raven.Imports.Newtonsoft.Json;

namespace Raven.Server.Json
{
    public unsafe class BlittableJsonWriter : IDisposable
    {

        private readonly RavenOperationContext _context;

        private readonly JsonReader _reader;
        private readonly UnmanagedWriteBuffer _stream;
        private int _bufferSize;
        private byte* _buffer;
        private int _position;
        private List<int> _escapePositions = new List<int>();
        public int DiscardedCompressions, Compressed;

        internal BlittableJsonWriter(JsonTextReader reader, RavenOperationContext context, string documentId)
        {
            _reader = reader;
            _stream = context.GetStream(documentId);
            _context = context;
        }

        public int SizeInBytes => _stream.SizeInBytes;

        public void Dispose()
        {
            _stream.Dispose();
        }

        private byte* GetTempBuffer(int minSize)
        {
            // enlarge buffer if needed
            if (minSize > _bufferSize)
            {
                _bufferSize = (int)Voron.Util.Utils.NearestPowerOfTwo(minSize);
                _buffer = _context.GetNativeTempBuffer(_bufferSize, out _bufferSize);
            }
            return _buffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CopyTo(byte* ptr)
        {
            return _stream.CopyTo(ptr);
        }

        /// <summary>
        /// Writes the json object from  reader received in the ctor into the received UnmanangedWriteBuffer
        /// </summary>
        public void Run()
        {
            if (_reader.Read() == false)
                throw new EndOfStreamException("Expected start of object, but got EOF");
            if (_reader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);
            BlittableJsonToken token;

            // Write the whole object recursively
            var rootOffset = WriteObject(out token);

            // Write the property names and register it's positions
            var propertyArrayOffset = new int[_context.CachedProperties.PropertiesDiscovered];
            for (var index = 0; index < propertyArrayOffset.Length; index++)
            {
                BlittableJsonToken _;
                propertyArrayOffset[index] = WriteString(_context.CachedProperties.GetProperty(index), out _, compress: false);
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

        /// <summary>
        /// Write an object to the UnamangedBuffer
        /// </summary>
        /// <param name="objectToken"></param>
        /// <returns></returns>
        private int WriteObject(out BlittableJsonToken objectToken)
        {
            var properties = new List<PropertyTag>();
            var firstWrite = _position;
            var maxPropId = -1;

            // Iterate through the object's properties, write it to the UnmanagedWriteBuffer and register it's names and positions
            while (true)
            {
                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected property name, but got EOF");

                if (_reader.TokenType == JsonToken.EndObject)
                    break;

                if (_reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);

                var propIndex = _context.CachedProperties.GetPropertyId((string)_reader.Value);

                maxPropId = Math.Max(maxPropId, propIndex);

                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected value, but got EOF");

                // Write object property into the UnmanagedWriteBuffer
                BlittableJsonToken token;
                var valuePos = WriteValue(out token);

                // Register property possition, name id (PropertyId) and type (object type and metadata)
                properties.Add(new PropertyTag
                {
                    Position = valuePos,
                    Type = (byte)token,
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

            _position += WriteVariableSizeNumber(properties.Count);

            // Write object metadata
            foreach (var sortedProperty in properties)
            {
                WriteNumber(objectMetadataStart - sortedProperty.Position, positionSize);
                WriteNumber(sortedProperty.PropertyId, propertyIdSize);
                _stream.WriteByte(sortedProperty.Type);
                _position += positionSize + propertyIdSize + sizeof(byte);
            }

            return objectMetadataStart;
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
        private int WriteValue(out BlittableJsonToken token)
        {
            var start = _position;
            switch (_reader.TokenType)
            {
                case JsonToken.StartObject:
                    return WriteObject(out token);

                case JsonToken.StartArray:
                    return WriteArray(out token);
                case JsonToken.Integer:
                    _position += WriteVariableSizeNumber((long)_reader.Value);
                    token = BlittableJsonToken.Integer;
                    return start;
                case JsonToken.Float:
                    BlittableJsonToken ignored;
                    var d = ((double)_reader.Value);
                    var str = d.ToString("r", CultureInfo.InvariantCulture);
                    WriteString(EnsureDecimalPlace(d, str), out ignored, compress: false);
                    token = BlittableJsonToken.Float;
                    return start;
                case JsonToken.String:
                    WriteString((string)_reader.Value, out token);
                    return start;
                case JsonToken.Boolean:
                    var value = (byte)((bool)_reader.Value ? 1 : 0);
                    _stream.WriteByte(value);
                    _position++;
                    token = BlittableJsonToken.Boolean;
                    return start;
                case JsonToken.Null:
                    token = BlittableJsonToken.Null;
                    return start; // nothing to do here, we handle that with the token
                case JsonToken.Undefined:
                    token = BlittableJsonToken.Null;
                    return start; // nothing to do here, we handle that with the token
                case JsonToken.Date:
                    var dateStr = ((DateTime)_reader.Value).GetDefaultRavenFormat();
                    WriteString(dateStr, out token);
                    return start;
                case JsonToken.Bytes:
                    throw new NotImplementedException("Writing bytes is not supported");
                // ReSharper disable RedundantCaseLabel
                case JsonToken.PropertyName:
                case JsonToken.None:
                case JsonToken.StartConstructor:
                case JsonToken.EndConstructor:
                case JsonToken.EndObject:
                case JsonToken.EndArray:
                case JsonToken.Raw:
                case JsonToken.Comment:
                default:
                    throw new InvalidDataException("Expected a value, but got " + _reader.TokenType);
                    // ReSharper restore RedundantCaseLabel
            }
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }
        private int WriteArray(out BlittableJsonToken arrayToken)
        {
            var positions = new List<int>();
            var types = new List<BlittableJsonToken>();
            while (true)
            {
                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected value, but got EOF");
                if (_reader.TokenType == JsonToken.EndArray)
                    break;


                BlittableJsonToken token;
                var pos = WriteValue(out token);
                types.Add(token);
                positions.Add(pos);
            }
            var arrayInfoStart = _position;
            arrayToken = BlittableJsonToken.StartArray;

            _position += WriteVariableSizeNumber(positions.Count);
            if (positions.Count == 0)
            {
                arrayToken |= BlittableJsonToken.OffsetSizeByte;
                return arrayInfoStart;
            }

            var distanceFromFirstItem = arrayInfoStart - positions[0];
            var distanceTypeSize = SetOffsetSizeFlag(ref arrayToken, distanceFromFirstItem);

            for (var i = 0; i < positions.Count; i++)
            {
                WriteNumber(arrayInfoStart - positions[i], distanceTypeSize);
                _position += distanceTypeSize;
            }

            for (var i = 0; i < types.Count; i++)
            {
                _stream.WriteByte((byte)types[i]);
                _position++;
            }

            return arrayInfoStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteString(string str, out BlittableJsonToken token, bool compress = true)
        {
            var startPos = _position;
            fixed (char* pChars = str)
            {
                token = BlittableJsonToken.String;

                var strByteCount = _context.Encoding.GetByteCount(pChars, str.Length);
                _position += WriteVariableSizeNumber(strByteCount);

                int bufferSize = strByteCount;
                var shouldCompress = compress && strByteCount > 128;
                if (shouldCompress)
                {
                    bufferSize += LZ4.MaximumOutputLength(strByteCount);
                }

                var buffer = GetTempBuffer(bufferSize);
                var byteLen = _context.Encoding.GetBytes(pChars, str.Length, buffer, _bufferSize);
                if (byteLen != strByteCount)
                    throw new FormatException("Calculated and real byte length did not match, should not happen");

                CheckStringEscapeSequences(buffer, byteLen);
                if (shouldCompress)
                {
                    var compressedSize = _context.Lz4.Encode64(buffer, buffer + byteLen, byteLen, _bufferSize - byteLen);
                    Compressed++;
                    // only if we actually save more than space
                    if (strByteCount > compressedSize + sizeof(int) * 2/*overhead of the compressed legnth*/)
                    {
                        token = BlittableJsonToken.CompressedString;
                        buffer += byteLen;
                        byteLen = compressedSize;
                        _position += WriteVariableSizeNumber(compressedSize);
                    }
                    else
                    {
                        DiscardedCompressions++;
                    }

                }

                _stream.Write(buffer, byteLen);
                _position += byteLen;
                // we write the number of the escape sequences required
                // and then we write the distance to the _next_ escape sequence
                _position += WriteVariableSizeNumber(_escapePositions.Count);
                if (_escapePositions.Count > 0)
                {
                    _position += WriteVariableSizeNumber(_escapePositions[0]);
                    for (int i = 1; i < _escapePositions.Count; i++)
                    {
                        _position += WriteVariableSizeNumber(_escapePositions[i] - _escapePositions[i - 1] -1);
                    }
                }
                return startPos;
            }
        }

        private void CheckStringEscapeSequences(byte* ptr, int len)
        {
            _escapePositions.Clear();
            for (int i = 0; i < len; i++)
            {
                switch (ptr[i])
                {
                    case (byte)'\b':
                    case (byte)'\t':
                    case (byte)'\n':
                    case (byte)'\f':
                    case (byte)'\r':
                    case (byte)'\\':
                    case (byte)'"':
                    case (byte)'\'':
                        _escapePositions.Add(i);
                        break;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            var buffer = GetTempBuffer(8);
            switch (sizeOfValue)
            {
                case sizeof(int):
                    buffer[0] = (byte)value;
                    buffer[1] = (byte)(value >> 8);
                    buffer[2] = (byte)(value >> 16);
                    buffer[3] = (byte)(value >> 24);
                    _stream.Write(buffer, 4);
                    break;
                case sizeof(short):
                    buffer[0] = (byte)value;
                    buffer[1] = (byte)(value >> 8);
                    _stream.Write(buffer, 2);
                    break;
                case sizeof(byte):
                    _stream.WriteByte((byte)value);
                    break;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteVariableSizeNumber(long value)
        {
            // see zig zap trick here:
            // https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
            // for negative values

            var buffer = GetTempBuffer(8);
            var count = 0;
            var v = (ulong) value;
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