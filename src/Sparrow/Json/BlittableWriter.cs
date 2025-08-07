using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        
        // A buffer allocated specifically for handling data compression tasks.
        // This buffer temporarily stores compressed data during serialization processes where string or data compression
        // is necessary to optimize memory usage and reduce data size.
        private AllocatedMemoryData _compressionBuffer;
        
        // A buffer used internally to store temporary data or intermediate results during write operations.
        // This buffer supports tasks such as variable-size encoding and other short-term data manipulations
        // that require efficient, reusable memory storage.
        private AllocatedMemoryData _innerBuffer;

        // Tracks the current writing position within the unmanaged buffer.
        // This field is important for maintaining proper offsets and managing data placement as data is written sequentially.
        private int _position;
        
        // Stores the size of the last data chunk written to the buffer.
        // This value assists in determining buffer requirements and optimizing memory allocation during context resets
        // or when reallocating buffers
        private int _lastSize;

        // Tracks a unique identifier for the current document being written within the context.
        // It ensures that the cached properties remain consistent and detects potential changes
        // or resets in context-related properties during the write process.
        // Initialized to -1 to indicate that it has not been set for the current session.
        private int _documentNumber = -1;
        
        public int Position => _position;

        public int SizeInBytes => _unmanagedWriteBuffer.SizeInBytes;

        public unsafe BlittableJsonReaderObject CreateReader()
        {
            _unmanagedWriteBuffer.EnsureSingleChunk(out byte* ptr, out int size);
            
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
                if (_documentNumber == -1)
                {
                    _documentNumber = _context.CachedProperties.DocumentNumber;
                    return _context.CachedProperties;
                }

                if (_documentNumber == _context.CachedProperties.DocumentNumber) 
                    return _context.CachedProperties;

                throw new InvalidOperationException($"The {_context.CachedProperties} were reset while building the document");
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
            _unmanagedWriteBuffer.Write<byte>(0);
            return startPos;
        }

        public int WriteValue(double value)
        {
            var s = EnsureDecimalPlace(value, value.ToString("R", CultureInfo.InvariantCulture));
            return WriteValue(s, out BlittableJsonToken _);
        }

        public int WriteValue(decimal value)
        {
            var s = EnsureDecimalPlace(value, value.ToString("G", CultureInfo.InvariantCulture));
            return WriteValue(s, out BlittableJsonToken _);
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
            _unmanagedWriteBuffer.Write(value);
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
            _innerBuffer ??= _context.GetMemory(32);
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        public unsafe WriteToken WriteObjectMetadata(FastList<AbstractBlittableJsonDocumentBuilder.PropertyTag> properties, long firstWrite, int maxPropId)
        {
            CachedProperties.Sort(properties);

            var objectMetadataStart = _position;
            var distanceFromFirstProperty = objectMetadataStart - firstWrite;

            // Find metadata size and properties offset and set appropriate flags in the BlittableJsonToken
            var objectToken = BlittableJsonToken.StartObject;
            var positionSize = SetOffsetSizeFlag(ref objectToken, distanceFromFirstProperty);
            var propertyIdSize = SetPropertyIdSizeFlag(ref objectToken, maxPropId);

            Debug.Assert(positionSize == sizeof(byte) || positionSize == sizeof(short) || positionSize == sizeof(int), $"Unsupported size {positionSize}");
            Debug.Assert(propertyIdSize == sizeof(byte) || propertyIdSize == sizeof(short) || propertyIdSize == sizeof(int), $"Unsupported size {propertyIdSize}");

            const int maxPropertySize = 2 * sizeof(int) + sizeof(byte);

            // PERF: By reserving on the stack the maximum size, we are able to avoid multiple calls to 
            // the unmanaged writer. This is a trade-off between stack usage and performance.
            var requiredMetadataBufferSize = maxPropertySize * properties.Count + VariableSizeEncoding.MaximumSizeOf<int>();

            // PERF: If the amount of properties is bigger than 512, we are facing a real outlier. This may happen
            // when we store a large dictionary. Therefore, we want to just deal with it differently than face a stackoverflow
            // even though the probability is quite low.
            Span<byte> metadataBuffer = properties.Count < 512 ? 
                                            stackalloc byte[requiredMetadataBufferSize] : 
                                            new byte[requiredMetadataBufferSize];

            // Write object metadata
            ref byte metadataStartPtr = ref MemoryMarshal.GetReference(metadataBuffer);
            ref byte metadataPtr = ref Unsafe.Add(ref metadataStartPtr, VariableSizeEncoding.Write(metadataBuffer, properties.Count));

            foreach (var sortedProperty in properties)
            {
                // PERF: We are using the known fact that it doesn't matter if the value is big or not because
                // the memory has already been reserved. Doing this, we avoid the branching associated with the
                // .WriteNumber() method. And since we know how many bytes we will write, we advance the pointer
                // appropriately.
                Unsafe.WriteUnaligned<int>(ref metadataPtr, objectMetadataStart - sortedProperty.Position);
                metadataPtr = ref Unsafe.Add(ref metadataPtr, positionSize);

                Unsafe.WriteUnaligned<int>(ref metadataPtr, sortedProperty.Property.PropertyId);
                metadataPtr = ref Unsafe.Add(ref metadataPtr, propertyIdSize);

                Unsafe.WriteUnaligned<byte>(ref metadataPtr, sortedProperty.Type);
                metadataPtr = ref Unsafe.Add(ref metadataPtr, 1);
            }
            
            int length = (int)Unsafe.ByteOffset(ref metadataStartPtr, ref metadataPtr);
            _unmanagedWriteBuffer.Write(metadataBuffer.Slice(0, length));
            _position += length;

            return new WriteToken(objectMetadataStart, objectToken);
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

                    _unmanagedWriteBuffer.Write((byte)types[i]);
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


        static BlittableWriter()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += CleanPropertyArrayOffset;
        }

        public static void CleanPropertyArrayOffset()
        {
            // Since we are releasing it because the current thread is no longer active,
            // we just nullify the reference and let the GC do its job.
            _intBuffer = null;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int WritePropertyNames(int rootOffset)
        {
            var cachedProperties = CachedProperties;
            int propertiesDiscovered = cachedProperties.PropertiesDiscovered;

            // Write the property names and register their positions
            Span<int> propertyArrayOffset = propertiesDiscovered < 512 ? 
                                            stackalloc int[propertiesDiscovered] : 
                                            new int[propertiesDiscovered];

            for (var index = 0; index < propertyArrayOffset.Length; index++)
            {
                var str = _context.GetLazyStringForFieldWithCaching(cachedProperties.GetProperty(index));
                if (str.EscapePositions == null || str.EscapePositions.Length == 0)
                {
                    propertyArrayOffset[index] = WriteValue(str.Buffer, str.Size);
                    continue;
                }

                propertyArrayOffset[index] = WriteValue(str.Buffer, str.Size, str.EscapePositions);
            }

            // Register the position of the properties offsets start
            var propertiesStart = _position;

            // Find the minimal space to store the offsets (byte,short,int) and raise the appropriate flag in the properties metadata
            BlittableJsonToken propertiesSizeMetadata = 0;
            var propertyNamesOffset = _position - rootOffset;
            var propertyArrayOffsetValueByteSize = SetOffsetSizeFlag(ref propertiesSizeMetadata, propertyNamesOffset);

            int maxPropertiesBufferLength = sizeof(byte) + propertiesDiscovered * sizeof(int);
            Span<byte> propertiesBuffer = propertiesDiscovered < 512 ? 
                                                stackalloc byte[maxPropertiesBufferLength] :
                                                new byte[maxPropertiesBufferLength];

            ref byte propertiesStartPtr = ref MemoryMarshal.GetReference(propertiesBuffer);
            ref byte propertiesPtr = ref propertiesStartPtr;


            Unsafe.WriteUnaligned<byte>(ref propertiesPtr, (byte)propertiesSizeMetadata);
            propertiesPtr = ref Unsafe.Add(ref propertiesPtr, 1);

            foreach (var propertyOffset in propertyArrayOffset)
            {
                // PERF: We are using the known fact that it doesn't matter if the value is big or not because
                // the memory has already been reserved. Doing this, we avoid the branching associated with the
                // .WriteNumber() method. And since we know how many bytes we will write, we advance the pointer
                // appropriately.
                Unsafe.WriteUnaligned<int>(ref propertiesPtr, propertiesStart - propertyOffset);
                propertiesPtr = ref Unsafe.Add(ref propertiesPtr, propertyArrayOffsetValueByteSize);
            }

            // Write property names offsets in the actual buffer.
            int length = (int)(Unsafe.ByteOffset(ref propertiesStartPtr, ref propertiesPtr));
            _unmanagedWriteBuffer.Write(propertiesBuffer.Slice(0, length));
            _position += length;

            return propertiesStart;
        }

        public void WriteDocumentMetadata(int rootOffset, BlittableJsonToken documentToken)
        {
            var propertiesStart = WritePropertyNames(rootOffset);

            WriteVariableSizeIntInReverse(rootOffset, propertiesStart);
            _unmanagedWriteBuffer.Write((byte)documentToken);
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
            _unmanagedWriteBuffer.Write((byte)value);
            if (sizeOfValue == sizeof(byte))
                return;

            _unmanagedWriteBuffer.Write((byte)(value >> 8));
            if (sizeOfValue == sizeof(ushort))
                return;

            _unmanagedWriteBuffer.Write((byte)(value >> 16));
            _unmanagedWriteBuffer.Write((byte)(value >> 24));
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeLong(long value)
        {
            // We will do zigzag encoding for int64, but not for int32
            var v = (ulong)((value << 1) ^ (value >> 63));

            // If value is 0, write a single byte and return
            if (v < 0x80)
            {
                _unmanagedWriteBuffer.Write((byte)v);
                return 1;
            }

            byte* dest = stackalloc byte[VariableSizeEncoding.MaximumSizeOf<long>()];
            var writtenBytes = VariableSizeEncoding.Write(dest, v);
            _unmanagedWriteBuffer.Write(dest, writtenBytes);
            return writtenBytes;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeInt(int value)
        {
            // If value is 0, write a single byte and return
            if (value is >= 0 and < 0x80)
            {
                _unmanagedWriteBuffer.Write((byte)value);
                return 1;
            }

            byte* dest = stackalloc byte[VariableSizeEncoding.MaximumSizeOf<int>()];
            var writtenBytes = VariableSizeEncoding.Write(dest, value);
            _unmanagedWriteBuffer.Write(dest, writtenBytes);
            return writtenBytes;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteVariableSizeIntInReverse(int value)
        {
            // If value is 0, write a single byte and return
            if (value is >= 0 and < 0x80)
            {
                _unmanagedWriteBuffer.Write((byte)value);
                return 1;
            }

            // Calculate the number of bytes needed
            int significantBits = 32 - Bits.LeadingZeroes((uint)value);
            int byteCount = (significantBits + 6) / 7;

            byte* dest = stackalloc byte[VariableSizeEncoding.MaximumSizeOf<int>()];
            
            byte* destPtr = dest + byteCount - 1;

            var v = (uint)value;
            while (v >= 0x80)
            {
                *destPtr = (byte)(v | 0x80);
                destPtr--;
                v >>= 7;
            }
            *destPtr = (byte)(v);

            _unmanagedWriteBuffer.Write(dest, byteCount);

            return byteCount;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void WriteVariableSizeIntInReverse(int v1, int v2)
        {
            // We will write in reverse, so we will grow the buffer from the end 
            // and use that to send to the unmanaged write buffer.

            // Calculate the maximum number of bytes needed
            int bufferSize = 2 * VariableSizeEncoding.MaximumSizeOf<int>();

            byte* dest = stackalloc byte[bufferSize];
            byte* destEnd = dest + bufferSize;
            byte* destPtr = destEnd - 1;

            // Since we are doing it the other way around, we will write the last first
            var v = (uint)v2;
            while (v >= 0x80)
            {
                *destPtr = (byte)(v | 0x80);
                destPtr--;
                v >>= 7;
            }
            *destPtr = (byte)(v);
            destPtr--;

            // Then we will write the first
            v = (uint)v1;
            while (v >= 0x80)
            {
                *destPtr = (byte)(v | 0x80);
                destPtr--;
                v >>= 7;
            }
            *destPtr = (byte)(v);

            // Then take the first byte and calculate the distance to the end and write it.
            _unmanagedWriteBuffer.Write(destPtr, (int)(destEnd - destPtr));
        }

#if NET7_0_OR_GREATER

        public static byte[] ByteActionTable =>
        [
            // 0-31: Control characters (action = 2)
            2, 2, 2, 2, 2, 2, 2, 2, // 0-7
            1, 1, 1, 2, 1, 1, 2, 2, // 8-15 (\b, \t, \n, _, \f, \r)
            2, 2, 2, 2, 2, 2, 2, 2, // 16-23
            2, 2, 2, 2, 2, 2, 2, 2, // 24-31

            // 32-63: Mostly normal characters (action = 0), except " at 34
            0, 0, 1, 0, 0, 0, 0, 0, // 32-39 (space, !, ", #, $, %, &, ')
            0, 0, 0, 0, 0, 0, 0, 0, // 40-47 ((, ), *, +, ,, -, ., /)
            0, 0, 0, 0, 0, 0, 0, 0, // 48-55 (0-7)
            0, 0, 0, 0, 0, 0, 0, 0, // 56-63 (8-9, :, ;, <, =, >, ?)

            // 64-95: Normal characters (action = 0), except \ at 92
            0, 0, 0, 0, 0, 0, 0, 0, // 64-71 (@, A-G)
            0, 0, 0, 0, 0, 0, 0, 0, // 72-79 (H-O)
            0, 0, 0, 0, 0, 0, 0, 0, // 80-87 (P-W)
            0, 0, 0, 0, 1, 0, 0, 0, // 88-95 (X, Y, Z, [, \, ], ^, _)

            // 96-127: Normal characters (action = 0)
            0, 0, 0, 0, 0, 0, 0, 0, // 96-103 (`, a-h)
            0, 0, 0, 0, 0, 0, 0, 0, // 104-111 (i-p)
            0, 0, 0, 0, 0, 0, 0, 0, // 112-119 (q-x)
            0, 0, 0, 0, 0, 0, 0, 0, // 120-127 (y-z, {, |, }, ~, DEL)

            // 128-255: UTF-8 continuation bytes or high ASCII (action = 0)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ];

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int WriteValueFromStack(ReadOnlySpan<byte> str, out BlittableJsonToken token)
        {
            // This should never trigger, if it does it mean the caller was modified incorrectly.
            Debug.Assert(str.Length < 512);

            int* escapePositions = stackalloc int[str.Length]; // Max escapes: one per char
            byte* buffer = stackalloc byte[str.Length * 6]; // Max size: 6 bytes per char

            byte* current = buffer; // Pointer to current position in output buffer
            int escapeCount = 0; // Number of simple escapes
            int lastEscape = 0; // Position after last escape for distance calculation

            // Get references for input string
            ref byte startRef = ref MemoryMarshal.GetReference(str);
            ref byte endRef = ref Unsafe.Add(ref startRef, str.Length); // Reference just past the end
            ref byte currentRef = ref startRef;

            // Process string using reference arithmetic
            while (Unsafe.IsAddressGreaterThan(ref endRef, ref currentRef))
            {
                byte value = currentRef; // Read current byte
                byte action = ByteActionTable[value];

                if (action == 0) // No escape
                {
                    *current = value;
                    current++;
                }
                else if (action == 1) // Simple escape
                {
                    *current = value;
                    int escapePos = (int)(current - buffer); // Current offset in output
                    escapePositions[escapeCount] = escapePos - lastEscape;
                    lastEscape = escapePos + 1;
                    escapeCount++;
                    current++;
                }
                else // action == 2, Control character
                {
                    *(ushort*)current = '\\' | ('u' << 8); // Write "\u"
                    *(int*)(current + 2) = AbstractBlittableJsonTextWriter.ControlCodeEscapes[value]; // 4 hex digits
                    current += 6; // Advance past 6 bytes
                }

                currentRef = ref Unsafe.Add(ref currentRef, 1); // Move to next byte in input
            }

            Debug.Assert((current - buffer) <= str.Length * 6, "We check that even a full escape characters string would respect this property.");

            token = BlittableJsonToken.String;
            int startPos = _position;

            int length = (int)(current - buffer);

            int posCount = 0;
            posCount += WriteVariableSizeInt(length); // Write length prefix
            _unmanagedWriteBuffer.Write(buffer, length); // Write the string data
            posCount += length;

            Debug.Assert(str.Length >= escapeCount, "We check that even a full escape characters string would respect this property.");

            posCount += WriteVariableSizeInt(escapeCount); // Write escape positions count
            for (int i = 0; i < escapeCount; i++)
                posCount += WriteVariableSizeInt(escapePositions[i]); // Write escape position sequence

            _position += posCount;

            return startPos;
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int WriteValueFromStack(ReadOnlySpan<char> str, out BlittableJsonToken token)
        {
            Span<byte> strBuffer = stackalloc byte[Encodings.Utf8.GetMaxByteCount(str.Length)];
            var stringSize = Encodings.Utf8.GetBytes(str, strBuffer);
            return WriteValueFromStack(strBuffer.Slice(0, stringSize), out token);
        }
#endif

        private unsafe int WriteValueFromHeap(ReadOnlySpan<byte> str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
            _intBuffer ??= new FastList<int>();

            var escapePositionsMaxSize = JsonParserState.FindEscapedPositionsMaxSize(str, out var _);
            int size = str.Length + escapePositionsMaxSize;

            AllocatedMemoryData buffer = null;
            try
            {
                buffer = _context.GetMemory(size);
                var bufferSpan = buffer.AsSpan();

                ref var bufferStart = ref MemoryMarshal.GetReference(bufferSpan);
                ref var srcStart = ref MemoryMarshal.GetReference(str);
                Unsafe.CopyBlock(ref bufferStart, ref srcStart, (uint)str.Length);

                int stringSize = str.Length;
                JsonParserState.FindEscapedPositionsAndEscapeControls(_intBuffer, buffer.Address, ref stringSize, escapePositionsMaxSize);
                return WriteValue(buffer.Address, stringSize, _intBuffer.AsUnsafeReadOnlySpan(), out token, mode, null);
            }
            finally
            {
                if (buffer != null)
                    _context.ReturnMemory(buffer);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(ReadOnlySpan<byte> str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
#if NET7_0_OR_GREATER
            if (str.Length <= 256 && mode is not (UsageMode.CompressSmallStrings or UsageMode.CompressStrings))
            {
                // PERF: Since we know the size, we can actually do this much more efficiently. 
                // even more if the caller is an actual string constant, which will cause the call to be inlined
                // as a constant value.
                return WriteValueFromStack(str, out token);
            }
#endif

            // PERF: This is the unoptimized version.
            return WriteValueFromHeap(str, out token, mode);
        }

#if NET7_0_OR_GREATER
        public int WriteValue(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
        {
            return WriteValue(str.AsSpan(), out token, mode);
        }

         [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteValue(ReadOnlySpan<char> str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
#else
        public int WriteValue(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
#endif
        {
#if NET7_0_OR_GREATER
            if (str.Length <= 256 && mode is (UsageMode.None or UsageMode.ValidateDouble))
            {
                // PERF: Since we know the size, we can actually do this much more efficiently. 
                // even more if the caller is an actual string constant, which will cause the call to be inlined
                // as a constant value.
                return WriteValueFromStack(str, out token);
            }
#endif

            // PERF: This is the unoptimized version.
            return WriteValueFromHeap(str, out token, mode);
        }

#if NET7_0_OR_GREATER
        private unsafe int WriteValueFromHeap(ReadOnlySpan<char> str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
#else
        private unsafe int WriteValueFromHeap(string str, out BlittableJsonToken token, UsageMode mode = UsageMode.None)
#endif
        {
            _intBuffer ??= new FastList<int>();

            var escapePositionsMaxSize = JsonParserState.FindMaxEscapedPositionAndControlCharSize(str, out _);
            int size = Encodings.Utf8.GetMaxByteCount(str.Length) + escapePositionsMaxSize;
            if (size > 8 * 1024 * 1024)
            {
                size = Encodings.Utf8.GetByteCount(str) + escapePositionsMaxSize;
            }

            AllocatedMemoryData buffer = null;
            try
            {
                buffer = _context.GetMemory(size);

#if NET7_0_OR_GREATER
                var stringSize = Encodings.Utf8.GetBytes(str, buffer.AsSpan());
#else
                var stringSize = Encodings.Utf8.GetBytes(str.AsSpan(), new Span<byte>(buffer.Address, size));
#endif
                JsonParserState.FindEscapedPositionsAndEscapeControls(_intBuffer, buffer.Address, ref stringSize, escapePositionsMaxSize);
                return WriteValue(buffer.Address, stringSize, _intBuffer.AsUnsafeReadOnlySpan(), out token, mode, null);                

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

        public unsafe int WriteValue(LazyStringValue str, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
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
        
        public unsafe int WriteValue(byte* buffer, int size, ReadOnlySpan<int> escapePositions, out BlittableJsonToken token, UsageMode mode, int? initialCompressedSize)
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

            if (escapePositions.IsEmpty)
            {
                position += WriteVariableSizeInt(0);
                goto Finish;
            }

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositions.Length);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach.
            foreach (var pos in escapePositions)
                position += WriteVariableSizeInt(pos);

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
            _unmanagedWriteBuffer.Write<byte>(0);
            writtenBytes += 1;

            _position += writtenBytes;

            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteValue(byte* buffer, int size, FastList<int> escapePositions)
        {
            int position = _position;

            int startPos = position;
            position += WriteVariableSizeInt(size);
            _unmanagedWriteBuffer.Write(buffer, size);
            position += size;

            if (escapePositions == null || escapePositions.Count == 0)
            {
                _unmanagedWriteBuffer.Write<byte>(0);
                _position = position + 1;
                return startPos;
            }

            int escapePositionCount = escapePositions.Count;

            // we write the number of the escape sequences required
            // and then we write the distance to the _next_ escape sequence
            position += WriteVariableSizeInt(escapePositionCount);

            // PERF: Use indexer to avoid the allocation and overhead of the foreach.
            for (int i = 0; i < escapePositionCount; i++)
                position += WriteVariableSizeInt(escapePositions[i]);

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

        public int WriteVector<T>(ReadOnlySpan<T> vector)
            where T : unmanaged
        {
            BlittableVectorType GetVectorType()
            {
                var type = typeof(T);
                if (type == typeof(sbyte))
                    return BlittableVectorType.SByte;
                if (type == typeof(short))
                    return BlittableVectorType.Int16;
                if (type == typeof(int))
                    return BlittableVectorType.Int32;
                if (type == typeof(long))
                    return BlittableVectorType.Int64;
                if (type == typeof(byte))
                    return BlittableVectorType.Byte;
                if (type == typeof(ushort))
                    return BlittableVectorType.UInt16;
                if (type == typeof(uint))
                    return BlittableVectorType.UInt32;
                if (type == typeof(ulong))
                    return BlittableVectorType.UInt64;
                if (type == typeof(float))
                    return BlittableVectorType.Float;
                if (type == typeof(double))
                    return BlittableVectorType.Double;
#if NET6_0_OR_GREATER
                if (type == typeof(Half))
                    return BlittableVectorType.Half;
#endif
                
                throw new NotSupportedException($"Type {type.Name} is not supported in vectors.");
            }
            
            var startPos = _position;
            BlittableVectorType vectorType = GetVectorType();

            // Prepare the header
            BlittableVectorHeader header = new(vectorType, vector.Length);

            // Write the header
            _unmanagedWriteBuffer.Write(in header);
            _position += Unsafe.SizeOf<BlittableVectorHeader>();

            // Write the vector data
            _unmanagedWriteBuffer.Write(vector);
            _position += Unsafe.SizeOf<T>() * vector.Length;
            
            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int TryCompressValue(ref byte* buffer, ref int position, int size, ref BlittableJsonToken token, UsageMode mode, int? initialCompressedSize, int maxGoodCompressionSize)
        {
            bool shouldCompress = initialCompressedSize.HasValue ||
                                  (((mode & UsageMode.CompressStrings) == UsageMode.CompressStrings) && (size > 128)) ||
                                  ((mode & UsageMode.CompressSmallStrings) == UsageMode.CompressSmallStrings) && (size <= 128);

            if (shouldCompress == false) 
                return size;

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
