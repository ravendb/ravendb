using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Util;

//using Raven.Imports.Newtonsoft.Json;

namespace Raven.Server.Json
{
    public unsafe class BlittableJsonWriter : IDisposable
    {
        private readonly RavenOperationContext _context;
        private readonly UnmanagedJsonParser _reader;
        private readonly UnmanagedWriteBuffer _stream;
        private int _bufferSize, _compressionBufferSize;
        private byte* _buffer, _compressionBuffer;

        private int _position;
        public int DiscardedCompressions, Compressed;

        internal BlittableJsonWriter(UnmanagedJsonParser reader, RavenOperationContext context, string documentId)
        {
            _reader = reader;
            _stream = context.GetStream(documentId);
            _context = context;
        }

        public int SizeInBytes => _stream.SizeInBytes;

        public void Dispose()
        {
            if (_buffer != null)
            {
                _context.Pool.ReturnMemory(_buffer);
                _buffer = null;
            }
            if (_compressionBuffer != null)
            {
                _context.Pool.ReturnMemory(_compressionBuffer);
                _compressionBuffer = null;
            }
        
            _stream.Dispose();
        }

        private byte* GetCompressionBuffer(int minSize)
        {
            // enlarge buffer if needed
            if (minSize > _compressionBufferSize)
            {
                _compressionBufferSize = (int)Voron.Util.Utils.NearestPowerOfTwo(minSize);
                _compressionBuffer = _context.Pool.GetMemory(_compressionBufferSize, out _compressionBufferSize);
            }
            return _compressionBuffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* GetTempBuffer(int minSize)
        {
            if (minSize <= _bufferSize)
                return _buffer;
            return IncreaseTempBufferSize(minSize);
        }

        private byte* IncreaseTempBufferSize(int minSize)
        {
            if (_buffer != null)
                _context.Pool.ReturnMemory(_buffer);
            _bufferSize = (int) Voron.Util.Utils.NearestPowerOfTwo(minSize);
            return _buffer = _context.Pool.GetMemory(_bufferSize, out _bufferSize);
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
            _reader.Read();
            if (_reader.Current != UnmanagedJsonParser.Tokens.StartObject)
                throw new InvalidDataException("Expected start of object, but got " + _reader.Current);
            BlittableJsonToken token;

            // Write the whole object recursively
            var rootOffset = WriteObject(out token);

            // Write the property names and register it's positions
            var propertyArrayOffset = new int[_context.CachedProperties.PropertiesDiscovered];
            for (var index = 0; index < propertyArrayOffset.Length; index++)
            {
                BlittableJsonToken _;
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
            var startPos = WriteString(prop, out token, false);
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
                _position += WriteVariableSizeInt(prop.EscapePositions[0]);
                for (int i = 1; i < prop.EscapePositions.Length; i++)
                {
                    _position += WriteVariableSizeInt(prop.EscapePositions[i] - prop.EscapePositions[i - 1] - 1);
                }
            }
            return startPos;
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
                _reader.Read();

                if (_reader.Current == UnmanagedJsonParser.Tokens.EndObject)
                    break;

                if (_reader.Current != UnmanagedJsonParser.Tokens.String)
                    throw new InvalidDataException("Expected property, but got " + _reader.Current);

                var buffer = GetTempBuffer(_reader.StringBuffer.SizeInBytes);
                _reader.StringBuffer.CopyTo(buffer);

                var property = new LazyStringValue(null, buffer, _reader.StringBuffer.SizeInBytes, _context);
                if (_reader.EscapePositions.Count > 0)
                {
                    property.EscapePositions = _reader.EscapePositions.ToArray();
                }
                var propIndex = _context.CachedProperties.GetPropertyId(property);
              
                maxPropId = Math.Max(maxPropId, propIndex);

                _reader.Read();

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

            _position += WriteVariableSizeInt(properties.Count);

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
            switch (_reader.Current)
            {
                case UnmanagedJsonParser.Tokens.StartObject:
                    return WriteObject(out token);
                case UnmanagedJsonParser.Tokens.StartArray:
                    return WriteArray(out token);
                case UnmanagedJsonParser.Tokens.Integer:
                    _position += WriteVariableSizeLong(_reader.Long);
                    token = BlittableJsonToken.Integer;
                    return start;
                case UnmanagedJsonParser.Tokens.Float:
                    BlittableJsonToken ignored;
                    WriteStringFromReader(out ignored);
                    token = BlittableJsonToken.Float;
                    return start;
                case UnmanagedJsonParser.Tokens.String:
                    WriteStringFromReader(out token);
                    return start;
                case UnmanagedJsonParser.Tokens.True:
                case UnmanagedJsonParser.Tokens.False:
                    _stream.WriteByte(_reader.Current == UnmanagedJsonParser.Tokens.True ? (byte)1 : (byte)0);
                    _position++;
                    token = BlittableJsonToken.Boolean;
                    return start;
                case UnmanagedJsonParser.Tokens.Null:
                    token = BlittableJsonToken.Null;
                    _stream.WriteByte(0);
                    _position++;
                    return start; // nothing to do here, we handle that with the token

                default:
                    throw new InvalidDataException("Expected a value, but got " + _reader.Current);
                    // ReSharper restore RedundantCaseLabel
            }
        }

        private void WriteStringFromReader(out BlittableJsonToken token)
        {
            var unmanagedWriteBuffer = _reader.StringBuffer;
            var buffer = GetTempBuffer(unmanagedWriteBuffer.SizeInBytes);
            unmanagedWriteBuffer.CopyTo(buffer);
            var str = new LazyStringValue(null, buffer, unmanagedWriteBuffer.SizeInBytes, _context);
            WriteString(str, out token);
            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(_reader.EscapePositions.Count);
            if (_reader.EscapePositions.Count > 0)
            {
                _position += WriteVariableSizeInt(_reader.EscapePositions[0]);
                for (int i = 1; i < _reader.EscapePositions.Count; i++)
                {
                    _position += WriteVariableSizeInt(_reader.EscapePositions[i] - _reader.EscapePositions[i - 1] - 1);
                }
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
                _reader.Read();
                if (_reader.Current == UnmanagedJsonParser.Tokens.EndArray)
                    break;


                BlittableJsonToken token;
                var pos = WriteValue(out token);
                types.Add(token);
                positions.Add(pos);
            }
            var arrayInfoStart = _position;
            arrayToken = BlittableJsonToken.StartArray;

            _position += WriteVariableSizeInt(positions.Count);
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

        public int WriteString(LazyStringValue str, out BlittableJsonToken token, bool compress = true)
        {
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(str.Size);
            var buffer = str.Buffer;
            var size = str.Size;
            if (compress && str.Size > 128)
            {
                var compressionBufferSize = LZ4.MaximumOutputLength(str.Size);
                var compressionBuffer = GetCompressionBuffer(compressionBufferSize);
                var compressedSize = _context.Lz4.Encode64(str.Buffer, compressionBuffer, str.Size,
                    compressionBufferSize);
                Compressed++;
                // only if we actually save more than space
                if (str.Size > compressedSize + sizeof(int) * 2 /*overhead of the compressed legnth*/)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            switch (sizeOfValue)
            {
                case sizeof(int):
                    var intBuffer = stackalloc byte[4];
                    intBuffer[0] = (byte)value;
                    intBuffer[1] = (byte)(value >> 8);
                    intBuffer[2] = (byte)(value >> 16);
                    intBuffer[3] = (byte)(value >> 24);
                    _stream.Write(intBuffer, 4);
                    break;
                case sizeof(short):
                    var shortBuffer = stackalloc byte[2];
                    shortBuffer[0] = (byte)value;
                    shortBuffer[1] = (byte)(value >> 8);
                    _stream.Write(shortBuffer, 2);
                    break;
                case sizeof(byte):
                    _stream.WriteByte((byte)value);
                    break;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteVariableSizeLong(long value)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteVariableSizeInt(int value)
        {
            // assume that we don't use negative values very often
            var count = 0;
            var v = (uint)value;
            while (v >= 0x80)
            {
                _stream.WriteByte((byte)(v | 0x80));
                v >>= 7;
            }
            _stream.WriteByte((byte)v);
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