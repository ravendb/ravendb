using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Compression;
using Sparrow.Json.Parsing;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public class BlittableWriter<TWriter> : IDisposable
        where TWriter : struct, IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private TWriter _unmanagedWriteBuffer;
        private AllocatedMemoryData _compressionBuffer;
        private int _position;

        public int Position => _position;

        public int SizeInBytes => _unmanagedWriteBuffer.SizeInBytes;

        private static readonly Encoding Utf8Encoding = Encoding.UTF8;

        public unsafe BlittableJsonReaderObject CreateReader()
        {
            byte* ptr;
            int size;
            _unmanagedWriteBuffer.EnsureSingleChunk(out ptr, out size);
            var reader = new BlittableJsonReaderObject(
                ptr, 
                size, 
                _context, 
                (UnmanagedWriteBuffer)(object)_unmanagedWriteBuffer);

            //we don't care to lose instance of write buffer,
            //since when context is reset, the allocated memory is "reclaimed"

            _unmanagedWriteBuffer = default(TWriter);
            return reader;
        }


        public BlittableWriter(JsonOperationContext context, TWriter writer)
        {
            _context = context;
            _unmanagedWriteBuffer = writer;
        }

        public BlittableWriter(JsonOperationContext context)
        {
            _context = context;
        }

        public unsafe void WriteValue(byte* p, int size) // blittable
        {
            _position += WriteVariableSizeInt(size);
            _unmanagedWriteBuffer.Write(p, size);
        }

        public int WriteValue(long value)
        {
            var startPos = _position;
            _position += WriteVariableSizeLong(value);
            return startPos;
        }

        public int WriteValue(bool value)
        {
            var startPos = _position;
            _position += WriteVariableSizeInt(value ? 1 : 0);
            return startPos;
        }

        public int WriteNull()
        {
            var startPos = _position++;
            _unmanagedWriteBuffer.WriteByte(0);
            return startPos;
        }

        public int WriteValue(double value)
        {
            // todo: write something more performant here..
            var s = EnsureDecimalPlace(value, value.ToString("R", CultureInfo.InvariantCulture));
            BlittableJsonToken token;
            return WriteValue(s, out token);
        }

        public int WriteValue(decimal value)
        {
            return WriteValue((double)value);
        }

        public int WriteValue(float value)
        {
            return WriteValue((double)value);
        }

        public int WriteValue(LazyDoubleValue value)
        {
            return WriteValue(value.Inner);
        }

        public int WriteValue(byte value)
        {
            var startPos = _position;
            _unmanagedWriteBuffer.WriteByte(value);
            _position++;
            return startPos;
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }

        public void Reset()
        {
            _unmanagedWriteBuffer.Dispose();
            if (_compressionBuffer != null)
            {
                _context.ReturnMemory(_compressionBuffer);
                _compressionBuffer = null;
            }
        }

        public void ResetAndRenew()
        {
            _unmanagedWriteBuffer.Dispose();
            _unmanagedWriteBuffer = (TWriter)(object)_context.GetStream();
            _position = 0;
        }

        public WriteToken WriteObjectMetadata(List<PropertyTag> properties, long firstWrite, int maxPropId)
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
            for (int i = 0; i < properties.Count; i++)
            {
                var sortedProperty = properties[i];

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

        public int WriteArrayMetadata(List<int> positions, List<BlittableJsonToken> types,
            ref BlittableJsonToken listToken)
        {
            var arrayInfoStart = _position;


            _position += WriteVariableSizeInt(positions.Count);
            if (positions.Count == 0)
            {
                listToken |= BlittableJsonToken.OffsetSizeByte;
            }
            else
            {
                var distanceFromFirstItem = arrayInfoStart - positions[0];
                var distanceTypeSize = SetOffsetSizeFlag(ref listToken, distanceFromFirstItem);

                for (var i = 0; i < positions.Count; i++)
                {
                    WriteNumber(arrayInfoStart - positions[i], distanceTypeSize);
                    _position += distanceTypeSize;

                    _unmanagedWriteBuffer.WriteByte((byte)types[i]);
                    _position++;
                }
            }
            return arrayInfoStart;
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

        public int WritePropertyNames(int rootOffset)
        {
            var cachedProperties = _context.CachedProperties;

            // Write the property names and register their positions
            var propertyArrayOffset = new int[cachedProperties.PropertiesDiscovered];

            unsafe
            {
                BlittableJsonToken _;
                for (var index = 0; index < propertyArrayOffset.Length; index++)
                {
                    var str = _context.GetLazyStringForFieldWithCaching(cachedProperties.GetProperty(index));
                    propertyArrayOffset[index] = WriteValue(str.Buffer, str.Size, str.EscapePositions, out _, UsageMode.None, null);
                }
            }

            // Register the position of the properties offsets start
            var propertiesStart = _position;

            // Find the minimal space to store the offsets (byte,short,int) and raise the appropriate flag in the properties metadata
            BlittableJsonToken propertiesSizeMetadata = 0;
            var propertyNamesOffset = _position - rootOffset;
            var propertyArrayOffsetValueByteSize = SetOffsetSizeFlag(ref propertiesSizeMetadata, propertyNamesOffset);

            WriteNumber((int)propertiesSizeMetadata, sizeof(byte));

            // Write property names offsets
            // PERF: Using for to avoid the cost of the enumerator.
            for (int i = 0; i < propertyArrayOffset.Length; i++)
            {
                int offset = propertyArrayOffset[i];
                WriteNumber(propertiesStart - offset, propertyArrayOffsetValueByteSize);
            }

            return propertiesStart;
        }

        public void WriteDocumentMetadata(int rootOffset, BlittableJsonToken documentToken)
        {
            var propertiesStart = WritePropertyNames(rootOffset);

            WriteVariableSizeIntInReverse(rootOffset);
            WriteVariableSizeIntInReverse(propertiesStart);
            WriteNumber((int)documentToken, sizeof(byte));
        }

        private static int SetOffsetSizeFlag(ref BlittableJsonToken objectToken, long distanceFromFirstProperty)
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
        public void WriteNumber(int value, int sizeOfValue)
        {
            // PERF: Instead of threw add this as a debug thing. We cannot afford this method not getting inlined.
            Debug.Assert(sizeOfValue == sizeof(byte) || sizeOfValue == sizeof(short) || sizeOfValue == sizeof(int), $"Unsupported size {sizeOfValue}");

            // PERF: With the current JIT at 12 of January of 2017 the switch statement dont get inlined.
            if (sizeOfValue == sizeof(int))
            {
                _unmanagedWriteBuffer.WriteByte((byte)value);
                _unmanagedWriteBuffer.WriteByte((byte)(value >> 8));
                _unmanagedWriteBuffer.WriteByte((byte)(value >> 16));
                _unmanagedWriteBuffer.WriteByte((byte)(value >> 24));
            }
            else if (sizeOfValue == sizeof(ushort))
            {
                _unmanagedWriteBuffer.WriteByte((byte)value);
                _unmanagedWriteBuffer.WriteByte((byte)(value >> 8));
            }
            else
            {
                _unmanagedWriteBuffer.WriteByte((byte)value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

            if (count == 1)
                _unmanagedWriteBuffer.WriteByte(*buffer);
            else
                _unmanagedWriteBuffer.Write(buffer, count);

            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

            if (count == 1)
                _unmanagedWriteBuffer.WriteByte(*buffer);
            else
                _unmanagedWriteBuffer.Write(buffer, count);

            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

            if (count == 1)
            {
                _unmanagedWriteBuffer.WriteByte(*buffer);
            }
            else
            {
                for (int i = count - 1; i >= count / 2; i--)
                {
                    var tmp = buffer[i];
                    buffer[i] = buffer[count - 1 - i];
                    buffer[count - 1 - i] = tmp;
                }
                _unmanagedWriteBuffer.Write(buffer, count);
            }

            return count;
        }

        [ThreadStatic]
        private static List<int> _intBuffer;

        public unsafe int WriteValue(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
            if (_intBuffer == null)
                _intBuffer = new List<int>();

            var escapePositionsMaxSize = JsonParserState.FindEscapePositionsMaxSize(str);
            int size = Encoding.UTF8.GetMaxByteCount(str.Length)
                       + escapePositionsMaxSize;

            AllocatedMemoryData buffer = null;
            try
            {
                buffer = _context.GetMemory(size);
                fixed (char* pChars = str)
                {
                    var stringSize = Utf8Encoding.GetBytes(pChars, str.Length, buffer.Address, size);
                    JsonParserState.FindEscapePositionsIn(_intBuffer, buffer.Address, stringSize, escapePositionsMaxSize);
                    return WriteValue(buffer.Address, stringSize, _intBuffer, out token, mode, null);
                }
            }
            finally
            {
                if(buffer != null)
                    _context.ReturnMemory(buffer);
            }
        }

        public int WriteValue(LazyStringValue str)
        {
            BlittableJsonToken token;
            return WriteValue(str, out token, UsageMode.None, null);
        }

        public unsafe int WriteValue(LazyStringValue str, out BlittableJsonToken token,
            UsageMode mode, int? initialCompressedSize)
        {
            return WriteValue(str.Buffer, str.Size, str.EscapePositions, out token, mode, initialCompressedSize);
        }

        public unsafe int WriteValue(byte* buffer, int size, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            int startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            _position += WriteVariableSizeInt(0);
            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, List<int> escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            if (escapePositions == null)
            {
                _position += WriteVariableSizeInt(0);
                return startPos;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(escapePositions.Count);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            int count = escapePositions.Count;
            for (int i = 0; i < count; i++)
                _position += WriteVariableSizeInt(escapePositions[i]);

            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, int[] escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            if (escapePositions == null)
            {
                _position += WriteVariableSizeInt(0);
                return startPos;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            _position += WriteVariableSizeInt(escapePositions.Length);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach. 
            int count = escapePositions.Length;
            for (int i = 0; i < count; i++)
                _position += WriteVariableSizeInt(escapePositions[i]);

            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int TryCompressValue(ref byte* buffer, int size, ref BlittableJsonToken token, UsageMode mode,
            int? initialCompressedSize, int maxGoodCompressionSize)
        {
            bool shouldCompress = initialCompressedSize.HasValue ||
                                  (((mode & UsageMode.CompressStrings) == UsageMode.CompressStrings) && (size > 128))
                                  ||
                                  ((mode & UsageMode.CompressSmallStrings) == UsageMode.CompressSmallStrings) && (size <= 128);

            if (shouldCompress)
            {
                int compressedSize;
                byte* compressionBuffer;
                if (initialCompressedSize.HasValue)
                {
                    // we already have compressed data here
                    compressedSize = initialCompressedSize.Value;
                    compressionBuffer = buffer;
                }
                else
                {
                    compressionBuffer = CompressBuffer(buffer, size, maxGoodCompressionSize, out compressedSize);
                }
                if (compressedSize > 0) // only if we actually save more than space
                {
                    token = BlittableJsonToken.CompressedString;
                    buffer = compressionBuffer;
                    size = compressedSize;
                    _position += WriteVariableSizeInt(compressedSize);
                }
            }
            return size;
        }

        private unsafe byte* CompressBuffer(byte* buffer, int size, int maxGoodCompressionSize, out int compressedSize)
        {
            var compressionBuffer = GetCompressionBuffer(size);
            if (size > 128)
            {
                compressedSize = LZ4.Encode64(buffer,
                    compressionBuffer,
                    size,
                    maxGoodCompressionSize,
                    acceleration: CalculateCompressionAcceleration(size));
            }
            else
            {
                compressedSize = SmallStringCompression.Instance.Compress(buffer,
                    compressionBuffer,
                    size,
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
                if (_compressionBuffer != null)
                    _context.ReturnMemory(_compressionBuffer);
                _compressionBuffer = _context.GetMemory(minSize);
            }
         
            return _compressionBuffer.Address;
        }

        public void Dispose()
        {
            _unmanagedWriteBuffer.Dispose();
            if (_compressionBuffer != null)
                _context.ReturnMemory(_compressionBuffer);

            _compressionBuffer = null;
        }
    }
}