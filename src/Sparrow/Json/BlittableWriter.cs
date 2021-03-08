using System;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Compression;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using static Sparrow.Json.BlittableJsonDocumentBuilder;

namespace Sparrow.Json
{
    public sealed class BlittableWriter<TWriter> : IDisposable
        where TWriter : struct, IUnmanagedWriteBuffer
    {
        private readonly JsonOperationContext _context;
        private TWriter _unmanagedWriteBuffer;
        private AllocatedMemoryData _compressionBuffer;
        private AllocatedMemoryData _innerBuffer;
        private int _position;
        private int _lastSize;
        private int _documentNumber = -1;
        public int Position => _position;

        public int SizeInBytes => _unmanagedWriteBuffer.SizeInBytes;

        public unsafe BlittableJsonReaderObject CreateReader()
        {
            byte* ptr;
            int size;
            _unmanagedWriteBuffer.EnsureSingleChunk(out ptr, out size);
            _lastSize = size;
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

        internal CachedProperties CachedProperties
        {
            get
            {
                ThrowIfCachedPropertiesWereReset();
                return _context.CachedProperties;
            }
        }

        public BlittableWriter(JsonOperationContext context, TWriter writer)
        {
            _context = context;
            _unmanagedWriteBuffer = writer;
            _innerBuffer = _context.GetMemory(32);
        }

        public BlittableWriter(JsonOperationContext context)
        {
            _context = context;
            _innerBuffer = _context.GetMemory(32);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(long value)
        {
            var startPos = _position;
            _position += WriteVariableSizeLong(value);
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(ulong value)
        {
            var s = value.ToString("G", CultureInfo.InvariantCulture);
            return WriteValue(s, out BlittableJsonToken token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            var s = EnsureDecimalPlace(value, value.ToString("R", CultureInfo.InvariantCulture));
            BlittableJsonToken token;
            return WriteValue(s, out token);
        }

        public int WriteValue(decimal value)
        {
            var s = EnsureDecimalPlace(value, value.ToString("G", CultureInfo.InvariantCulture));
            BlittableJsonToken token;
            return WriteValue(s, out token);
        }

        public int WriteValue(float value)
        {
            return WriteValue((double)value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(LazyNumberValue value)
        {
            return WriteValue(value.Inner);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(byte value)
        {
            var startPos = _position;
            _unmanagedWriteBuffer.WriteByte(value);
            _position++;
            return startPos;
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || double.IsNegativeInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }

        private static string EnsureDecimalPlace(decimal value, string text)
        {
            if (text.IndexOf('.') != -1)
                return text;

            return text + ".0";
        }

        public void Reset()
        {
            _documentNumber = -1;
            _unmanagedWriteBuffer.Dispose();
            if (_compressionBuffer != null)
            {
                _context.ReturnMemory(_compressionBuffer);
                _compressionBuffer = null;
            }
            if (_innerBuffer != null)
            {
                _context.ReturnMemory(_innerBuffer);
                _innerBuffer = null;
            }
        }

        public void ResetAndRenew()
        {
            _documentNumber = -1;
            _unmanagedWriteBuffer.Dispose();
            _unmanagedWriteBuffer = (TWriter)(object)_context.GetStream(_lastSize);
            _position = 0;
            if (_innerBuffer == null)
                _innerBuffer = _context.GetMemory(32);
        }

        public WriteToken WriteObjectMetadata(FastList<AbstractBlittableJsonDocumentBuilder.PropertyTag> properties, long firstWrite, int maxPropId)
        {
            CachedProperties.Sort(properties);

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
                WriteNumber(sortedProperty.Property.PropertyId, propertyIdSize);
                _unmanagedWriteBuffer.WriteByte(sortedProperty.Type);
                _position += positionSize + propertyIdSize + sizeof(byte);
            }

            return new WriteToken
            {
                ValuePos = objectMetadataStart,
                WrittenToken = objectToken
            };
        }

        public int WriteArrayMetadata(FastList<int> positions, FastList<BlittableJsonToken> types, ref BlittableJsonToken listToken)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SetPropertyIdSizeFlag(ref BlittableJsonToken objectToken, int maxPropId)
        {
            if (maxPropId <= byte.MaxValue)
            {
                objectToken |= BlittableJsonToken.PropertyIdSizeByte;
                return sizeof(byte);
            }

            if (maxPropId <= ushort.MaxValue)
            {
                objectToken |= BlittableJsonToken.PropertyIdSizeShort;
                return sizeof(short);
            }

            objectToken |= BlittableJsonToken.PropertyIdSizeInt;
            return sizeof(int);
        }

        [ThreadStatic]
        private static FastList<int> _intBuffer;

        [ThreadStatic]
        private static int[] _propertyArrayOffset;

        static BlittableWriter()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += CleanPropertyArrayOffset;
        }

        public static void CleanPropertyArrayOffset()
        {
            _propertyArrayOffset = null;
            _intBuffer?.Clear();
            _intBuffer = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int WritePropertyNames(int rootOffset)
        {
            var cachedProperties = CachedProperties;
            int propertiesDiscovered = cachedProperties.PropertiesDiscovered;

            // Write the property names and register their positions
            if (_propertyArrayOffset == null || _propertyArrayOffset.Length < propertiesDiscovered)
            {
                _propertyArrayOffset = new int[Bits.PowerOf2(propertiesDiscovered)];
            }

            unsafe
            {
                for (var index = 0; index < propertiesDiscovered; index++)
                {
                    var str = _context.GetLazyStringForFieldWithCaching(cachedProperties.GetProperty(index));
                    if (str.EscapePositions == null || str.EscapePositions.Length == 0)
                    {
                        _propertyArrayOffset[index] = WriteValue(str.Buffer, str.Size);
                        continue;
                    }

                    _propertyArrayOffset[index] = WriteValue(str.Buffer, str.Size, str.EscapePositions);
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
            for (int i = 0; i < propertiesDiscovered; i++)
            {
                int offset = _propertyArrayOffset[i];
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SetOffsetSizeFlag(ref BlittableJsonToken objectToken, long distanceFromFirstProperty)
        {
            if (distanceFromFirstProperty <= byte.MaxValue)
            {
                objectToken |= BlittableJsonToken.OffsetSizeByte;
                return sizeof(byte);
            }

            if (distanceFromFirstProperty <= ushort.MaxValue)
            {
                objectToken |= BlittableJsonToken.OffsetSizeShort;
                return sizeof(short);
            }

            objectToken |= BlittableJsonToken.OffsetSizeInt;
            return sizeof(int);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            // PERF: Instead of threw add this as a debug thing. We cannot afford this method not getting inlined.
            Debug.Assert(sizeOfValue == sizeof(byte) || sizeOfValue == sizeof(short) || sizeOfValue == sizeof(int), $"Unsupported size {sizeOfValue}");

            // PERF: With the current JIT at 12 of January of 2017 the switch statement dont get inlined.
            _unmanagedWriteBuffer.WriteByte((byte)value);
            if (sizeOfValue == sizeof(byte))
                return;

            _unmanagedWriteBuffer.WriteByte((byte)(value >> 8));
            if (sizeOfValue == sizeof(ushort))
                return;

            _unmanagedWriteBuffer.WriteByte((byte)(value >> 16));
            _unmanagedWriteBuffer.WriteByte((byte)(value >> 24));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeLong(long value)
        {
            // see zig zap trick here:
            // https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
            // for negative values

            var buffer = _innerBuffer.Address;
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
            var buffer = _innerBuffer.Address;

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
            var buffer = _innerBuffer.Address;
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

        public unsafe int WriteValue(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
            if (_intBuffer == null)
                _intBuffer = new FastList<int>();

            var escapePositionsMaxSize = JsonParserState.FindEscapePositionsMaxSize(str, out _);
            int size = Encodings.Utf8.GetMaxByteCount(str.Length) + escapePositionsMaxSize;

            AllocatedMemoryData buffer = null;
            try
            {
                buffer = _context.GetMemory(size);

                var stringSize = Encodings.Utf8.GetBytes(str.AsSpan(), buffer.AsSpan());
                JsonParserState.FindEscapePositionsIn(_intBuffer, buffer.Address, ref stringSize, escapePositionsMaxSize);
                return WriteValue(buffer.Address, stringSize, _intBuffer, out token, mode, null);                
            }
            finally
            {
                if (buffer != null)
                    _context.ReturnMemory(buffer);
            }
        }

        public int WriteValue(LazyStringValue str)
        {
            return WriteValue(str, out _, UsageMode.None, null);
        }

        public unsafe int WriteValue(LazyStringValue str, out BlittableJsonToken token,
            UsageMode mode, int? initialCompressedSize)
        {
            if (str.EscapePositions != null)
            {
                return WriteValue(str.Buffer, str.Size, str.EscapePositions, out token, mode, initialCompressedSize);
            }
            // else this is a raw value
            var startPos = _position;
            token = BlittableJsonToken.String;

            _position += WriteVariableSizeInt(str.Size);

            var escapeSequencePos = GetSizeIncludingEscapeSequences(str.Buffer, str.Size);
            _unmanagedWriteBuffer.Write(str.Buffer, escapeSequencePos);
            _position += escapeSequencePos;
            return startPos;
        }

        private static unsafe int GetSizeIncludingEscapeSequences(byte* buffer, int size)
        {
            var escapeSequencePos = size;
            // now need to also include the size of the escape positions
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
            for (int i = 0; i < numberOfEscapeSequences; i++)
            {
                BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
            }
            return escapeSequencePos;
        }

        public unsafe int WriteValue(LazyCompressedStringValue str, out BlittableJsonToken token,
            UsageMode mode)
        {
            var startPos = _position;
            token = BlittableJsonToken.CompressedString;

            _position += WriteVariableSizeInt(str.UncompressedSize);

            _position += WriteVariableSizeInt(str.CompressedSize);

            var escapeSequencePos = GetSizeIncludingEscapeSequences(str.Buffer, str.CompressedSize);
            _unmanagedWriteBuffer.Write(str.Buffer, escapeSequencePos);
            _position += escapeSequencePos;
            return startPos;
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
                size = TryCompressValue(ref buffer, ref _position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            _position += WriteVariableSizeInt(0);
            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, FastList<int> escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
        {
            int position = _position;

            int startPos = position;
            token = BlittableJsonToken.String;

            position += WriteVariableSizeInt(size);

            // if we are more than this size, we want to abort the compression early and just use
            // the verbatim string
            int maxGoodCompressionSize = size - sizeof(int) * 2;
            if (maxGoodCompressionSize > 0)
            {
                size = TryCompressValue(ref buffer, ref position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
            }

            _unmanagedWriteBuffer.Write(buffer, size);
            position += size;

            if (escapePositions == null)
            {
                position += WriteVariableSizeInt(0);
                goto Finish;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositions.Count);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach.
            int count = escapePositions.Count;
            for (int i = 0; i < count; i++)
                position += WriteVariableSizeInt(escapePositions[i]);

            Finish:
            _position = position;
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteValue(byte* buffer, int size)
        {
            int startPos = _position;

            int writtenBytes = WriteVariableSizeInt(size);
            _unmanagedWriteBuffer.Write(buffer, size);
            writtenBytes += size;
            writtenBytes += WriteVariableSizeInt(0);

            _position += writtenBytes;

            return startPos;
        }

        public unsafe int WriteValue(byte* buffer, int size, int[] escapePositions)
        {
            var startPos = _position;
            _position += WriteVariableSizeInt(size);
            _unmanagedWriteBuffer.Write(buffer, size);
            _position += size;

            if (escapePositions == null || escapePositions.Length == 0)
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

        public unsafe int WriteValue(byte* buffer, int size, FastList<int> escapePositions)
        {
            int position = _position;

            int startPos = position;
            position += WriteVariableSizeInt(size);
            _unmanagedWriteBuffer.Write(buffer, size);
            position += size;

            if (escapePositions == null || escapePositions.Count == 0)
            {
                position += WriteVariableSizeInt(0);
                goto Finish;
            }

            int escapePositionCount = escapePositions.Count;

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositionCount);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach.
            for (int i = 0; i < escapePositionCount; i++)
                position += WriteVariableSizeInt(escapePositions[i]);

            Finish:
            _position = position;
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
                size = TryCompressValue(ref buffer, ref _position, size, ref token, mode, initialCompressedSize, maxGoodCompressionSize);
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
        private unsafe int TryCompressValue(ref byte* buffer, ref int position, int size, ref BlittableJsonToken token, UsageMode mode, int? initialCompressedSize, int maxGoodCompressionSize)
        {
            bool shouldCompress = initialCompressedSize.HasValue ||
                                  (((mode & UsageMode.CompressStrings) == UsageMode.CompressStrings) && (size > 128)) ||
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
                    position += WriteVariableSizeInt(compressedSize);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CalculateCompressionAcceleration(int size)
        {
            return Bits.CeilLog2(size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        internal void ThrowIfCachedPropertiesWereReset()
        {
            if (_documentNumber == -1)
            {
                _documentNumber = _context.CachedProperties.DocumentNumber;
            }
            else if (_documentNumber != _context.CachedProperties.DocumentNumber)
            {
                throw new InvalidOperationException($"The {_context.CachedProperties} were reset while building the document");
            }
        }

        public void Dispose()
        {
            _unmanagedWriteBuffer.Dispose();

            if (_compressionBuffer != null)
                _context.ReturnMemory(_compressionBuffer);

            if (_innerBuffer != null)
                _context.ReturnMemory(_innerBuffer);

            _compressionBuffer = null;
            _innerBuffer = null;
        }
    }
}
