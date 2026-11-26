using System;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Compression;
using Sparrow.Extensions;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract unsafe class AbstractBlittableJsonTextWriter : IBlittableJsonTextWriter
    {
        protected readonly JsonOperationContext _context;
        protected readonly Stream _stream;
        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';

        public static ReadOnlySpan<byte> NewLineBuffer => "\r\n"u8;
        public static ReadOnlySpan<byte> NaNBuffer => "\"NaN\""u8;
        public static ReadOnlySpan<byte> PositiveInfinityBuffer => "\"Infinity\""u8;
        public static ReadOnlySpan<byte> NegativeInfinityBuffer => "\"-Infinity\""u8;

        public static readonly byte[] NullBuffer = "null"u8.ToArray();
        public static readonly byte[] TrueBuffer = "true"u8.ToArray();
        public static readonly byte[] FalseBuffer = "false"u8.ToArray();

        /// <summary><![CDATA[
        /// The original code that generates this flatten sequence.
        /// ControlCodeEscapes = new byte[32][];
        /// for (int i = 0; i< 32; i++)
        /// {
        ///    ControlCodeEscapes[i] = Encodings.Utf8.GetBytes(i.ToString("X4"));
        /// }
        /// 
        /// ]]></summary>
        private static ReadOnlySpan<byte> _controlCodeEscapes => "0000000100020003000400050006000700080009000A000B000C000D000E000F0010001100120013001400150016001700180019001A001B001C001D001E001F"u8;

        internal static ReadOnlySpan<int> ControlCodeEscapes => MemoryMarshal.Cast<byte, int>(_controlCodeEscapes);

        /// <summary><![CDATA[
        /// The original code that generates this flatten sequence.
        /// EscapeCharacters = new byte[256];
        /// for (int i = 0; i< 32; i++)
        ///     EscapeCharacters[i] = 0;
        ///
        /// for (int i = 32; i<EscapeCharacters.Length; i++)
        ///     EscapeCharacters[i] = 255;
        ///
        /// EscapeCharacters[(byte)'\b'] = (byte)'b';
        /// EscapeCharacters[(byte)'\t'] = (byte)'t';
        /// EscapeCharacters[(byte)'\n'] = (byte)'n';
        /// EscapeCharacters[(byte)'\f'] = (byte)'f';
        /// EscapeCharacters[(byte)'\r'] = (byte)'r';
        /// EscapeCharacters[(byte)'\\'] = (byte)'\\';
        /// EscapeCharacters[(byte)'/'] = (byte)'/';
        /// EscapeCharacters[(byte)'"'] = (byte)'"';
        /// ]]></summary>
        private static ReadOnlySpan<byte> EscapeCharacters => new byte[]
        {
              0,   0,   0,   0,   0,   0,   0,   0,  98, 116, 110,   0, 102, 114,   0,   0,
              0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0, 
            255, 255,  34, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  47,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  92, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        };

        private protected readonly JsonOperationContext.MemoryBuffer _pinnedBuffer;
        private readonly byte* _buffer;

        private protected int _pos;
        private readonly JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;

        // the name given to a property filter, valid only during the call (the filters only compare it)
        private LazyStringValue _filterPropertyName;

        protected AbstractBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;

            _returnBuffer = context.GetMemoryBuffer(out _pinnedBuffer);
            _buffer = _pinnedBuffer.Address;
        }

        public int Position => _pos;

        public override string ToString()
        {
            return Encodings.Utf8.GetString(_buffer, _pos);
        }

        public void WriteObject(BlittableJsonReaderObject obj)
        {
            if (obj == null)
            {
                WriteNull();
                return;
            }

            WriteObject(obj.GetView());
        }

        private void WriteArrayToStream(BlittableJsonReaderArray blittableArray)
        {
            var root = blittableArray.GetView(out var offsetSize);
            WriteArray(root, blittableArray.DataStart, offsetSize);
        }

        /// <summary>
        /// Writes the properties of obj in the order of their names, without the braces, skipping the ones the filter matches.
        /// </summary>
        /// <returns>true if a property was written</returns>
        internal bool WritePropertiesWithFilter(in BlittableJsonReaderObject.View obj, Func<LazyStringValue, bool> filter)
        {
            var properties = obj.GetProperties(out var count);
            var propertyEntrySize = obj.PropertyEntrySize;
            var first = true;
            for (int i = 0; i < count; i++)
            {
                var entry = properties + i * propertyEntrySize;
                if (filter != null)
                {
                    obj.GetProperty(entry, out _, out var propertyId);
                    var name = obj.GetPropertyName(propertyId, out var nameSize);
                    _filterPropertyName ??= new LazyStringValue(null, null, 0, _context);
                    _filterPropertyName.Renew(null, name, nameSize, _context);
                    if (filter(_filterPropertyName))
                        continue;
                }

                if (first == false)
                    WriteComma();
                first = false;

                WriteProperty(obj, entry);
            }

            return first == false;
        }

        private void WriteObject(in BlittableJsonReaderObject.View obj)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            WriteStartObject();
            WriteProperties(obj, skipIndex: -1);
            WriteEndObject();
        }

        private const int MaxPropertiesOnStack = 64;

        /// <summary>
        /// Writes the properties of obj in insertion order, without the braces, skipping the property at skipIndex.
        /// </summary>
        /// <returns>true if a property was written</returns>
#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        internal bool WriteProperties(in BlittableJsonReaderObject.View obj, int skipIndex)
        {
            var properties = obj.GetProperties(out var count);
            if (count == 0)
                return false;

            // the order and the offsets it is sorted by; every nesting level has its own
            int[] rented = null;
            Span<int> buffer = count <= MaxPropertiesOnStack
                ? stackalloc int[2 * MaxPropertiesOnStack]
                : rented = ArrayPool<int>.Shared.Rent(2 * count);
            try
            {
                var order = buffer.Slice(0, count);
                obj.GetPropertiesByInsertionOrder(order, buffer.Slice(count, count));

                var propertyEntrySize = obj.PropertyEntrySize;
                var first = true;
                foreach (var index in order)
                {
                    if (index == skipIndex)
                        continue;

                    if (first == false)
                        WriteComma();
                    first = false;

                    WriteProperty(obj, properties + index * propertyEntrySize);
                }

                return first == false;
            }
            finally
            {
                if (rented != null)
                    ArrayPool<int>.Shared.Return(rented);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteProperty(in BlittableJsonReaderObject.View obj, byte* entry)
        {
            var type = obj.GetProperty(entry, out var position, out var propertyId);

            var name = obj.GetPropertyName(propertyId, out var nameSize);
            WriteString(name, nameSize, skipEscaping: false);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;

            WriteValue(obj, type, position);
        }

        private void WriteArray(in BlittableJsonReaderObject.View root, byte* dataStart, int offsetSize)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            var count = VariableSizeEncoding.Read<int>(dataStart, out var countSize);
            var items = dataStart + countSize;
            var dataStartPosition = (int)(dataStart - root.Mem);

            WriteStartArray();
            for (int i = 0; i < count; i++)
            {
                if (i != 0)
                    WriteComma();

                var entry = items + i * (offsetSize + 1);
                var type = (BlittableJsonToken)entry[offsetSize];
                WriteValue(root, type, dataStartPosition - BlittableJsonReaderBase.ReadNumber(entry, offsetSize));
            }

            WriteEndArray();
        }

        private void WriteValue(in BlittableJsonReaderObject.View root, BlittableJsonToken type, int position)
        {
            var value = root.Mem + position;
            int size, offset;
            switch (type & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.String:
                    size = VariableSizeEncoding.Read<int>(value, out offset);
                    WriteString(value + offset, size, skipEscaping: false);
                    break;

                case BlittableJsonToken.Integer:
                    WriteInteger(ZigZagEncoding.Decode<long>(value, out _));
                    break;

                case BlittableJsonToken.LazyNumber:
                    size = VariableSizeEncoding.Read<int>(value, out offset);
                    WriteLazyNumber(value + offset, size);
                    break;

                case BlittableJsonToken.StartObject:
                    WriteObject(root.Nested(position, type));
                    break;

                case BlittableJsonToken.EmbeddedBlittable:
                    size = VariableSizeEncoding.Read<int>(value, out offset);
                    WriteObject(BlittableJsonReaderObject.View.ForRoot(value + offset, size));
                    break;

                case BlittableJsonToken.StartArray:
                    WriteArray(root, value, BlittableJsonReaderBase.ProcessTokenOffsetFlags(type));
                    break;

                case BlittableJsonToken.CompressedString:
                    var uncompressedSize = VariableSizeEncoding.Read<int>(value, out offset);
                    value += offset;
                    var compressedSize = VariableSizeEncoding.Read<int>(value, out offset);
                    WriteCompressedString(value + offset, uncompressedSize, compressedSize);
                    break;

                case BlittableJsonToken.Boolean:
                    WriteBool(*value == 1);
                    break;

                case BlittableJsonToken.Null:
                    WriteNull();
                    break;

                case BlittableJsonToken.RawBlob:
                    size = VariableSizeEncoding.Read<int>(value, out offset);
                    WriteRawString(value + offset, size);
                    break;

                case BlittableJsonToken.Vector:
                    WriteVectorToStream(new BlittableJsonReaderVector(value, root.Size - position, _context));
                    break;

                default:
                    ThrowUnidentifiedType(type);
                    break;
            }
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void ThrowUnidentifiedType(BlittableJsonToken type)
        {
            throw new DataMisalignedException($"Unidentified Type {type}");
        }

        public void WriteValue(BlittableJsonToken token, object val)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (token)
            {
                case BlittableJsonToken.String:
                    WriteString((LazyStringValue)val);
                    break;

                case BlittableJsonToken.Integer:
                    WriteInteger((long)val);
                    break;

                case BlittableJsonToken.StartArray:
                    WriteArrayToStream((BlittableJsonReaderArray)val);
                    break;

                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.StartObject:
                    var blittableJsonReaderObject = (BlittableJsonReaderObject)val;
                    WriteObject(blittableJsonReaderObject);
                    break;

                case BlittableJsonToken.CompressedString:
                    WriteString((LazyCompressedStringValue)val);
                    break;

                case BlittableJsonToken.LazyNumber:
                    WriteDouble((LazyNumberValue)val);
                    break;

                case BlittableJsonToken.Boolean:
                    WriteBool((bool)val);
                    break;

                case BlittableJsonToken.Null:
                    WriteNull();
                    break;

                case BlittableJsonToken.RawBlob:
                    var blob = (BlittableJsonReaderObject.RawBlob)val;
                    WriteRawString(blob.Address, blob.Length);
                    break;

                case BlittableJsonToken.Vector:
                    WriteVectorToStream((BlittableJsonReaderVector)val);
                    break;
                
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        private void WriteVectorToStream(BlittableJsonReaderVector vectorReader)
        {

            void WriteValues<T>(ReadOnlySpan<T> values) 
                where T : unmanaged
            {
                for (var i = 0; i < values.Length; i++)
                {
                    if (i != 0)
                    {
                        WriteComma();
                    }

                    if (typeof(T) == typeof(float))
                    {
                        WriteDouble((float)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(double))
                    {
                        WriteDouble((double)(object)values[i]);
                    }
#if NET6_0_OR_GREATER
                    else if (typeof(T) == typeof(Half))
                    {
                        WriteDouble((double)(object)values[i]);
                    }
#endif
                    else if (typeof(T) == typeof(sbyte))
                    {
                        WriteInteger((sbyte)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(byte))
                    {
                        WriteInteger((byte)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(short))
                    {
                        WriteInteger((short)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(int))
                    {
                        WriteInteger((int)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(ushort))
                    {
                        WriteInteger((ushort)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(uint))
                    {
                        WriteInteger((uint)(object)values[i]);
                    }
                    else if (typeof(T) == typeof(ulong))
                    {
                        WriteInteger((ulong)(object)values[i]);
                    }
                    else
                    {
                        WriteInteger((long)(object)values[i]);
                    }
                }
            }


            WriteStartArray();

            switch (vectorReader.Type)
            {
                case BlittableVectorType.SByte:
                    WriteValues(vectorReader.ReadArray<sbyte>());
                    break;
                case BlittableVectorType.Int16:
                    WriteValues(vectorReader.ReadArray<short>());
                    break;
                case BlittableVectorType.Int32:
                    WriteValues(vectorReader.ReadArray<int>());
                    break;
                case BlittableVectorType.Int64:
                    WriteValues(vectorReader.ReadArray<long>());
                    break;
                case BlittableVectorType.Byte:
                    WriteValues(vectorReader.ReadArray<byte>());
                    break;
                case BlittableVectorType.UInt16:
                    WriteValues(vectorReader.ReadArray<ushort>());
                    break;
                case BlittableVectorType.UInt32:
                    WriteValues(vectorReader.ReadArray<uint>());
                    break;
                case BlittableVectorType.UInt64:
                    WriteValues(vectorReader.ReadArray<ulong>());
                    break;
#if NET6_0_OR_GREATER
                case BlittableVectorType.Half:
                    WriteValues(vectorReader.ReadArray<Half>()); break;
#endif
                case BlittableVectorType.Float:
                    WriteValues(vectorReader.ReadArray<float>());
                    break;
                case BlittableVectorType.Double:
                    WriteValues(vectorReader.ReadArray<double>());
                    break;
            }

            WriteEndArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteDateTime(DateTime? value, bool isUtc)
        {
            if (value == null)
            {
                WriteNull();
                return 0;
            }

            return WriteDateTime(value.Value, isUtc);
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteDateTime(DateTime value, bool isUtc)
        {
            var auxBuffer = stackalloc byte[32];

            int size = value.GetDefaultRavenFormat(auxBuffer, 32, isUtc);

            WriteRawStringWhichMustBeWithoutEscapeChars(auxBuffer, size);

            return size;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        public void WriteString(string str, bool skipEscaping = false)
        {
            if (str == null)
            {
                WriteNull();
                return;
            }

            const int stackBufferSize = 768;
            var maxByteCount = Encodings.Utf8.GetMaxByteCount(str.Length);
            if (maxByteCount <= stackBufferSize)
            {
                var stackBuffer = stackalloc byte[stackBufferSize];
                WriteString(str, stackBuffer, stackBufferSize, skipEscaping);
                return;
            }

            var buffer = ArrayPool<byte>.Shared.Rent(maxByteCount);
            try
            {
                fixed (byte* pBuffer = buffer)
                    WriteString(str, pBuffer, buffer.Length, skipEscaping);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private void WriteString(string str, byte* buffer, int bufferSize, bool skipEscaping)
        {
            int size;
            fixed (char* chars = str)
                size = Encodings.Utf8.GetBytes(chars, str.Length, buffer, bufferSize);

            if (size == 1 && IsControlCodeCharacter(*buffer))
            {
                WriteControlCodeCharacter(*buffer);
                return;
            }

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;

            var remaining = new ReadOnlySpan<byte>(buffer, size);
            if (skipEscaping == false)
            {
                int index;
                while ((index = StringUtils.IndexOfEscapeCharacter(remaining)) != -1)
                {
                    WriteRawString(remaining.Slice(0, index));
                    WriteEscapeCharacter(_buffer, remaining[index]);
                    remaining = remaining.Slice(index + 1);
                }
            }

            WriteRawString(remaining);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsControlCodeCharacter(byte b) => b < 32 || b is >= 127 and <= 159;

        private void WriteControlCodeCharacter(byte b)
        {
            EnsureBuffer(8);
            _buffer[_pos++] = Quote;
            _buffer[_pos++] = (byte)'\\';
            _buffer[_pos++] = (byte)'u';
            _buffer[_pos++] = (byte)'0';
            _buffer[_pos++] = (byte)'0';
            _buffer[_pos++] = HexDigits[b >> 4];
            _buffer[_pos++] = HexDigits[b & 0xF];
            _buffer[_pos++] = Quote;
        }

        private static ReadOnlySpan<byte> HexDigits => "0123456789ABCDEF"u8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyStringValue str, bool skipEscaping = false)
        {
            if (str == null)
            {
                WriteNull();
                return;
            }

            WriteString(str.Buffer, str.Size, skipEscaping);
        }

        /// <summary>
        /// Writes a string whose escape positions are stored after it, as in a blittable.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteString(byte* strBuffer, int size, bool skipEscaping)
        {
            if (size == 1 && IsControlCodeCharacter(*strBuffer))
            {
                WriteControlCodeCharacter(*strBuffer);
                return;
            }

            var escapeSequencePos = size;
            var numberOfEscapeSequences = skipEscaping ? 0 : BlittableJsonReaderBase.ReadVariableSizeInt(strBuffer, ref escapeSequencePos);

            // We ensure our buffer will have enough space to deal with the whole string.

            const int NumberOfQuotesChars = 2; // for " "

            int bufferSize = 2 * numberOfEscapeSequences + size + NumberOfQuotesChars;
            if (bufferSize >= JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                UnlikelyWriteLargeString(strBuffer, size, numberOfEscapeSequences, escapeSequencePos); // OK, do it the slow way.
                return;
            }

            EnsureBuffer(size + NumberOfQuotesChars);
            _buffer[_pos++] = Quote;

            if (numberOfEscapeSequences == 0 && size < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                // PERF: Fast Path.
                Memory.Copy(_buffer + _pos, strBuffer, size);
                _pos += size;
            }
            else
            {
                UnlikelyWriteEscapeSequences(strBuffer, size, numberOfEscapeSequences, escapeSequencePos);
            }

            _buffer[_pos++] = Quote;
        }

        private void UnlikelyWriteEscapeSequences(byte* strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos)
        {
            // We ensure our buffer will have enough space to deal with the whole string.
            int bufferSize = 2 * numberOfEscapeSequences + size + 1;

            EnsureBuffer(bufferSize);

            var ptr = strBuffer;
            var buffer = _buffer;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;

                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, ref escapeSequencePos);
                if (bytesToSkip > 0)
                {
                    WriteRawString(strBuffer, bytesToSkip);
                    strBuffer += bytesToSkip;
                    size -= bytesToSkip;
                }

                var escapeCharacter = *strBuffer++;
                WriteEscapeCharacter(buffer, escapeCharacter);

                size--;
            }

            Debug.Assert(size >= 0);

            // write remaining (or full string) to the buffer in one shot
            if (size > 0)
                WriteRawString(strBuffer, size);
        }

        private void UnlikelyWriteLargeString(byte* strBuffer, int size, int numberOfEscapeSequences, int escapeSequencePos)
        {
            var ptr = strBuffer;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;

            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, ref escapeSequencePos);

                UnlikelyWriteLargeRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);

                WriteEscapeCharacter(_buffer, b);
            }

            // write remaining (or full string) to the buffer in one shot
            UnlikelyWriteLargeRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteEscapeCharacter(byte* buffer, byte b)
        {
            // We don't need to call it multiple times, we can just ensure we have enough and be done with it.
            EnsureBuffer(6);

            byte r = EscapeCharacters[b];
            buffer[_pos++] = (byte)'\\';
            buffer[_pos++] = r == 0 ? (byte)'u' : r;  // PERF: In .Net 8.0 this is a conditional move.

            if (r == 0)
            {
                *(int*)(buffer + _pos) = ControlCodeEscapes[b];
                _pos += 4;
                return;
            }

            if (r != 255)
                return;

            ThrowInvalidEscapeCharacter(b);
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private void ThrowInvalidEscapeCharacter(byte b)
        {
            throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
        }

        public void WriteString(LazyCompressedStringValue str)
        {
            WriteCompressedString(str.Buffer, str.UncompressedSize, str.CompressedSize);
        }

        private void WriteCompressedString(byte* strSrcBuffer, int size, int compressedSize)
        {
            var decompressed = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                fixed (byte* decompressedBuffer = decompressed)
                {
                    var strBuffer = decompressedBuffer;
                    LazyCompressedStringValue.Decompress(strSrcBuffer, compressedSize, strBuffer, size);

                    var escapeSequencePos = compressedSize;
                    var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer, ref escapeSequencePos);

                    // We ensure our buffer will have enough space to deal with the whole string.
                    int bufferSize = 2 * numberOfEscapeSequences + size + 2;
                    if (bufferSize >= JsonOperationContext.MemoryBuffer.DefaultSize)
                    {
                        UnlikelyWriteLargeString(numberOfEscapeSequences, strSrcBuffer, escapeSequencePos, strBuffer, size); // OK, do it the slow way instead.
                        return;
                    }

                    EnsureBuffer(bufferSize);

                    _buffer[_pos++] = Quote;
                    while (numberOfEscapeSequences > 0)
                    {
                        numberOfEscapeSequences--;
                        var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer, ref escapeSequencePos);

                        WriteRawString(strBuffer, bytesToSkip);

                        strBuffer += bytesToSkip;
                        size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                        var b = *(strBuffer++);

                        WriteEscapeCharacter(_buffer, b);
                    }

                    // write remaining (or full string) to the buffer in one shot
                    WriteRawString(strBuffer, size);

                    _buffer[_pos++] = Quote;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(decompressed);
            }
        }

        private void UnlikelyWriteLargeString(int numberOfEscapeSequences, byte* strSrcBuffer, int escapeSequencePos, byte* strBuffer, int size)
        {
            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);

                WriteEscapeCharacter(_buffer, b);
            }

            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteRawStringWhichMustBeWithoutEscapeChars(byte* buffer, int size)
        {
            EnsureBuffer(size + 2);
            _buffer[_pos++] = Quote;

            if (size + 2 < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                Memory.Copy(_buffer + _pos, buffer, size);
                _pos += size;
                _buffer[_pos++] = Quote;
            }
            else
            {
                UnlikelyWriteLargeRawString(buffer, size);
                _buffer[_pos++] = Quote;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteRawString(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                EnsureBuffer(buffer.Length);
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(_buffer + _pos), ref MemoryMarshal.GetReference(buffer), (uint)buffer.Length);
                _pos += buffer.Length;
                return;
            }

            UnlikelyWriteLargeRawString(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteRawString(byte* buffer, int size)
        {
            if (size < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                EnsureBuffer(size);
                Memory.Copy(_buffer + _pos, buffer, size);
                _pos += size;
                return;
            }

            UnlikelyWriteLargeRawString(buffer, size);
        }

        private void UnlikelyWriteLargeRawString(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);
            ref byte destination = ref Unsafe.AsRef<byte>(_buffer);

            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < buffer.Length)
            {
                var amountToCopy = Math.Min(buffer.Length - posInStr, JsonOperationContext.MemoryBuffer.DefaultSize - _pos);
                if (amountToCopy == 0)
                    goto End; // There is no space available to copy anything, let's just skip and move to flush. 

                Unsafe.CopyBlockUnaligned(
                    ref Unsafe.AddByteOffset(ref destination, (uint)_pos),
                    ref Unsafe.AddByteOffset(ref bufferStart, (uint)posInStr),
                    (uint)amountToCopy);

                posInStr += amountToCopy;
                _pos += amountToCopy;

                // We are not gonna waste a buffer flush if we still have space for other things.
                // Therefore, we will check if we are done (which is fast) and just break out if
                // that's the case.
                if (posInStr == buffer.Length)
                    break;

                End:
                FlushInternal();
            }
        }

        private void UnlikelyWriteLargeRawString(byte* buffer, int size)
        {
            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < size)
            {
                var amountToCopy = Math.Min(size - posInStr, JsonOperationContext.MemoryBuffer.DefaultSize - _pos);
                if (amountToCopy == 0)
                    goto End; // There is no space available to copy anything, let's just skip and move to flush. 

                Memory.Copy(_buffer + _pos, buffer + posInStr, amountToCopy);
                posInStr += amountToCopy;
                _pos += amountToCopy;

                // We are not gonna waste a buffer flush if we still have space for other things.
                // Therefore, we will check if we are done (which is fast) and just break out if
                // that's the case.
                if (posInStr == size)
                    break;

                End:
                FlushInternal();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteStartArray()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = StartArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteEndObject()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = EndObject;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureBuffer(int len)
        {
            // If len is bigger than the default size, then we will fail this check either way.
            // therefore, we can remove a check from the fast-path even if highly predictable.
            if (_pos + len < JsonOperationContext.MemoryBuffer.DefaultSize)
                return;

            if (len < JsonOperationContext.MemoryBuffer.DefaultSize)
            {
                FlushInternal();
                return;
            }

            ThrowValueTooBigForBuffer(len);
        }

        protected virtual bool FlushInternal()
        {
            if (_stream == null)
                ThrowStreamClosed();

            if (_pos == 0)
                return false;

            _stream.Write(_pinnedBuffer.Memory.Memory.Span.Slice(0, _pos));
            _stream.Flush();

            _pos = 0;
            return true;
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void ThrowValueTooBigForBuffer(int len)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len", len, "Length value too big: " + len);
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private void ThrowStreamClosed()
        {
            throw new ObjectDisposedException("The stream was closed already.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNull()
        {
            EnsureBuffer(4);

            Unsafe.WriteUnaligned(_buffer + _pos, Unsafe.ReadUnaligned<int>(ref NullBuffer[0]));
            _pos += sizeof(int);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBool(bool val)
        {
            EnsureBuffer(5);

            if (val)
            {
                Unsafe.WriteUnaligned(_buffer + _pos, Unsafe.ReadUnaligned<int>(ref TrueBuffer[0]));
                _pos += sizeof(int);
            }
            else
            {
                Unsafe.WriteUnaligned(_buffer + _pos, Unsafe.ReadUnaligned<int>(ref FalseBuffer[0]));
                _pos += sizeof(int);
                _buffer[_pos++] = FalseBuffer[sizeof(int)];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteComma()
        {
            EnsureBuffer(1);
            Unsafe.WriteUnaligned(_buffer + _pos, Comma);
            _pos++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(ReadOnlySpan<byte> prop)
        {
            WriteRawString(prop);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(LazyStringValue prop)
        {
            WriteString(prop);

            EnsureBuffer(1);
            Unsafe.WriteUnaligned(_buffer + _pos, Colon);
            _pos++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(string prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);

            EnsureBuffer(1);
            Unsafe.WriteUnaligned(_buffer + _pos, Colon);
            _pos++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(StringSegment prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);

            EnsureBuffer(1);
            Unsafe.WriteUnaligned(_buffer + _pos, Colon);
            _pos++;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        public void WriteInteger(long val)
        {
            if (val == 0)
            {
                EnsureBuffer(1);
                Unsafe.WriteUnaligned(_buffer + _pos, (byte)'0');
                _pos++;

                return;
            }

            var localBuffer = stackalloc byte[32];

            int idx = 0;
            var negative = false;
            var isLongMin = false;
            if (val < 0)
            {
                negative = true;
                if (val == long.MinValue)
                {
                    isLongMin = true;
                    val = long.MaxValue;
                }
                else
                    val = -val; // value is positive now.
            }

            do
            {
                var v = val % 10;
                if (isLongMin)
                {
                    isLongMin = false;
                    v += 1;
                }

                localBuffer[idx++] = (byte)('0' + v);
                val /= 10;
            } while (val != 0);

            if (negative)
                localBuffer[idx++] = (byte)'-';

            EnsureBuffer(idx);

            var buffer = _buffer;
            int auxPos = _pos;

            do
                buffer[auxPos++] = localBuffer[--idx];
            while (idx > 0);

            _pos = auxPos;
        }

        public void WriteInteger(ulong val)
        {
            if (val == 0)
            {
                EnsureBuffer(1);
                Unsafe.WriteUnaligned(_buffer + _pos, (byte)'0');
                _pos++;

                return;
            }

            var localBuffer = stackalloc byte[32];

            int idx = 0;

            do
            {
                var v = val % 10;
                localBuffer[idx++] = (byte)('0' + v);
                val /= 10;
            } while (val != 0);

            EnsureBuffer(idx);

            var buffer = _buffer;
            int auxPos = _pos;

            do
                buffer[auxPos++] = localBuffer[--idx];
            while (idx > 0);

            _pos = auxPos;
        }
        
        public void WriteDouble(LazyNumberValue val)
        {
            if (val.IsNaN())
            {
                WriteBufferFor(NaNBuffer);
                return;
            }

            if (val.IsPositiveInfinity())
            {
                WriteBufferFor(PositiveInfinityBuffer);
                return;
            }

            if (val.IsNegativeInfinity())
            {
                WriteBufferFor(NegativeInfinityBuffer);
                return;
            }

            var lazyStringValue = val.Inner;
            EnsureBuffer(lazyStringValue.Size);
            WriteRawString(lazyStringValue.Buffer, lazyStringValue.Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteLazyNumber(byte* number, int size)
        {
            // only NaN and the infinities, or an exponent after the first digit, have a byte above '9' in the first two
            if (size >= 3 && (number[0] > '9' || number[1] > '9') && TryWriteNonFiniteNumber(new ReadOnlySpan<byte>(number, size)))
                return;

            EnsureBuffer(size);
            WriteRawString(number, size);
        }

        private bool TryWriteNonFiniteNumber(ReadOnlySpan<byte> number)
        {
            if (number.SequenceEqual("NaN"u8))
            {
                WriteBufferFor(NaNBuffer);
                return true;
            }

            if (number.SequenceEqual("Infinity"u8))
            {
                WriteBufferFor(PositiveInfinityBuffer);
                return true;
            }

            if (number.SequenceEqual("-Infinity"u8))
            {
                WriteBufferFor(NegativeInfinityBuffer);
                return true;
            }

            return false;
        }

        public void WriteBufferFor(ReadOnlySpan<byte> buffer)
        {
            EnsureBuffer(buffer.Length);
            buffer.CopyTo(new Span<byte>(_buffer + _pos, buffer.Length));
            _pos += buffer.Length;
        }

#if NET6_0_OR_GREATER
        [SkipLocalsInit]
#endif
        public void WriteDouble(double val)
        {
            if (double.IsNaN(val))
            {
                WriteBufferFor(NaNBuffer);
                return;
            }

            if (double.IsPositiveInfinity(val))
            {
                WriteBufferFor(PositiveInfinityBuffer);
                return;
            }

            if (double.IsNegativeInfinity(val))
            {
                WriteBufferFor(NegativeInfinityBuffer);
                return;
            }

            Span<byte> buffer = stackalloc byte[32];
            if (Utf8Formatter.TryFormat(val, buffer, out int bytesWritten) is false)
            {
                throw new InvalidOperationException("Somehow we failed to write double (max 25 chars) to a 32 bytes buffer?! " + val);
            }
            EnsureBuffer(bytesWritten);
            WriteRawString(buffer.Slice(0, bytesWritten));
        }

        protected void DisposeInternal()
        {
            try
            {
                FlushInternal();
                _stream.Flush();
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so this exception doesn't matter
            }
            // TODO: remove when we update to .net core 3
            // https://github.com/dotnet/corefx/issues/36141
            catch (NotSupportedException e)
            {
                throw new IOException("The stream was closed by the peer.", e);
            }
            finally
            {
                _returnBuffer.Dispose();
            }
        }

        public void WriteNewLine()
        {
            EnsureBuffer(2);
            Unsafe.WriteUnaligned(_buffer + _pos, Unsafe.ReadUnaligned<short>(ref MemoryMarshal.GetReference(NewLineBuffer)));
            _pos += sizeof(short);
        }

        public void WriteMemoryChunk(IntPtr ptr, int size)
        {
            WriteMemoryChunk((byte*)ptr.ToPointer(), size);
        }

        public void WriteMemoryChunk(byte* ptr, int size)
        {
            var leftToWrite = size;
            if (leftToWrite >= JsonOperationContext.MemoryBuffer.DefaultSize - _pos)
                FlushInternal();

            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(JsonOperationContext.MemoryBuffer.DefaultSize - _pos, leftToWrite);
                Memory.Copy(_buffer + _pos, ptr + totalWritten, toWrite);
                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                FlushInternal();
            }
        }
    }
}
