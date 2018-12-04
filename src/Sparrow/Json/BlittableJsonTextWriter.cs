using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Sparrow.Extensions;

namespace Sparrow.Json
{
    public class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter
    {
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken) : base(context, context.CheckoutMemoryStream())
        {
            _outputStream = stream;
            _cancellationToken = cancellationToken;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeOuterFlushAsync()
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            if (innerStream.Length * 2 <= innerStream.Capacity)
                return new ValueTask<int>(0);

            Flush();
            return new ValueTask<int>(OuterFlushAsync());
        }

        public async Task<int> OuterFlushAsync()
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            Flush();
            innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            await _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, _cancellationToken).ConfigureAwait(false);
            innerStream.SetLength(0);
            return bytesCount;
        }

        public int OuterFlush()
        {
            var innerStream = _stream as MemoryStream;
            if (innerStream == null)
                ThrowInvalidTypeException(_stream?.GetType());
            Flush();
            innerStream.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            _outputStream.Write(bytes.Array, bytes.Offset, bytesCount);
            _stream.SetLength(0);
            return bytesCount;
        }

        public override void Dispose()
        {
            base.Dispose();
            OuterFlush();
            _context.ReturnMemoryStream((MemoryStream)_stream);
        }

        private void ThrowInvalidTypeException(Type typeOfStream)
        {
            throw new ArgumentException($"Expected stream to be MemoryStream, but got {(typeOfStream == null ? "null" : typeOfStream.ToString())}.");
        }
    }

    public class BlittableJsonTextWriter : AbstractBlittableJsonTextWriter
    {
        public BlittableJsonTextWriter(JsonOperationContext context, Stream stream) : base(context, stream)
        {
        }
    }

    public abstract unsafe class AbstractBlittableJsonTextWriter : IDisposable
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
        public static readonly byte[] NaNBuffer = { (byte)'"', (byte)'N', (byte)'a', (byte)'N', (byte)'"' };
        public static readonly byte[] PositiveInfinityBuffer =
        {
            (byte)'"', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };
        public static readonly byte[] NegativeInfinityBuffer =
        {
            (byte)'"', (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private static readonly byte[] EscapeCharacters;
        public static readonly byte[][] ControlCodeEscapes;

        private int _pos;
        private readonly byte* _buffer;
        private JsonOperationContext.ReturnBuffer _returnBuffer;
        private readonly JsonOperationContext.ManagedPinnedBuffer _pinnedBuffer;
        private readonly AllocatedMemoryData _parserAuxiliarMemory;

        static AbstractBlittableJsonTextWriter()
        {
            ControlCodeEscapes = new byte[32][];

            for (int i = 0; i < 32; i++)
            {
                ControlCodeEscapes[i] = Encodings.Utf8.GetBytes(i.ToString("X4"));
            }

            EscapeCharacters = new byte[256];
            for (int i = 0; i < 32; i++)
                EscapeCharacters[i] = 0;

            for (int i = 32; i < EscapeCharacters.Length; i++)
                EscapeCharacters[i] = 255;

            EscapeCharacters[(byte)'\b'] = (byte)'b';
            EscapeCharacters[(byte)'\t'] = (byte)'t';
            EscapeCharacters[(byte)'\n'] = (byte)'n';
            EscapeCharacters[(byte)'\f'] = (byte)'f';
            EscapeCharacters[(byte)'\r'] = (byte)'r';
            EscapeCharacters[(byte)'\\'] = (byte)'\\';
            EscapeCharacters[(byte)'/'] = (byte)'/';
            EscapeCharacters[(byte)'"'] = (byte)'"';
        }

        public AbstractBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;

            _returnBuffer = context.GetManagedBuffer(out _pinnedBuffer);
            _buffer = _pinnedBuffer.Pointer;

            _parserAuxiliarMemory = context.GetMemory(32);
        }

        public int Position => _pos;

        public override string ToString()
        {
            return Encodings.Utf8.GetString(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pos);
        }

        public void WriteObject(BlittableJsonReaderObject obj)
        {
            if (obj == null)
            {
                WriteNull();
                return;
            }

            WriteStartObject();
            var props = obj.GetPropertiesByInsertionOrder();
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < props.Length; i++)
            {
                if (i != 0)
                {
                    WriteComma();
                }

                obj.GetPropertyByIndex(props[i], ref prop);
                WritePropertyName(prop.Name);

                WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
            }

            WriteEndObject();
        }

        private void WriteArrayToStream(BlittableJsonReaderArray blittableArray)
        {
            WriteStartArray();
            var length = blittableArray.Length;
            for (var i = 0; i < length; i++)
            {
                var propertyValueAndType = blittableArray.GetValueTokenTupleByIndex(i);

                if (i != 0)
                {
                    WriteComma();
                }
                // write field value
                WriteValue(propertyValueAndType.Item2, propertyValueAndType.Item1);

            }
            WriteEndArray();
        }

        public void WriteValue(BlittableJsonToken token, object val)
        {
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
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteDateTime(DateTime value, bool isUtc)
        {
            int size = value.GetDefaultRavenFormat(_parserAuxiliarMemory, isUtc);

            var strBuffer = _parserAuxiliarMemory.Address;

            WriteRawStringWhichMustBeWithoutEscapeChars(strBuffer, size);
        }

        public void WriteString(string str, bool skipEscaping = false)
        {
            using (var lazyStr = _context.GetLazyString(str))
            {
                WriteString(lazyStr, skipEscaping);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyStringValue str, bool skipEscaping = false)
        {
            if (str == null)
            {
                WriteNull();
                return;
            }

            var size = str.Size;

            if (size == 1 && str.IsControlCodeCharacter(out var b))
            {
                WriteString($@"\u{b:X4}", skipEscaping: true);
                return;
            }

            var strBuffer = str.Buffer;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = skipEscaping ? 0 : BlittableJsonReaderBase.ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);

            // We ensure our buffer will have enough space to deal with the whole string.

            const int NumberOfQuotesChars = 2; // for " "

            int bufferSize = 2 * numberOfEscapeSequences + size + NumberOfQuotesChars;
            if (bufferSize >= JsonOperationContext.ManagedPinnedBuffer.Size)
            {
                UnlikelyWriteLargeString(strBuffer, size, numberOfEscapeSequences, escapeSequencePos); // OK, do it the slow way. 
                return;
            }

            EnsureBuffer(size + NumberOfQuotesChars);
            _buffer[_pos++] = Quote;

            if (numberOfEscapeSequences == 0)
            {
                // PERF: Fast Path. 
                WriteRawString(strBuffer, size);
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

                var auxPos = _pos;
                WriteEscapeCharacter(buffer, escapeCharacter, ref auxPos);
                _pos = auxPos;

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

                WriteEscapeCharacter(_buffer, b, ref _pos);
            }

            // write remaining (or full string) to the buffer in one shot
            UnlikelyWriteLargeRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteEscapeCharacter(byte* buffer, byte b, ref int pos)
        {
            byte r = EscapeCharacters[b];
            if (r == 0)
            {
                EnsureBuffer(6);
                buffer[pos++] = (byte)'\\';
                buffer[pos++] = (byte)'u';
                fixed (byte* esc = ControlCodeEscapes[b])
                    Memory.Copy(buffer + pos, esc, 4);
                pos += 4;
                return;
            }

            if (r != 255)
            {
                EnsureBuffer(2);
                buffer[pos++] = (byte)'\\';
                buffer[pos++] = r;
                return;
            }

            ThrowInvalidEscapeCharacter(b);
        }

        private void ThrowInvalidEscapeCharacter(byte b)
        {
            throw new InvalidOperationException("Invalid escape char '" + (char)b + "' numeric value is: " + b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyCompressedStringValue str)
        {
            var strBuffer = str.DecompressToTempBuffer(out AllocatedMemoryData allocated, _context);

            try
            {
                var strSrcBuffer = str.Buffer;

                var size = str.UncompressedSize;
                var escapeSequencePos = str.CompressedSize;
                var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(strSrcBuffer, ref escapeSequencePos);

                // We ensure our buffer will have enough space to deal with the whole string.
                int bufferSize = 2 * numberOfEscapeSequences + size + 2;
                if (bufferSize >= JsonOperationContext.ManagedPinnedBuffer.Size)
                    goto WriteLargeCompressedString; // OK, do it the slow way instead.

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

                    var auxPos = _pos;
                    WriteEscapeCharacter(_buffer, b, ref auxPos);
                    _pos = auxPos;
                }

                // write remaining (or full string) to the buffer in one shot
                WriteRawString(strBuffer, size);

                _buffer[_pos++] = Quote;

                return;

            WriteLargeCompressedString:
                UnlikelyWriteLargeString(numberOfEscapeSequences, strSrcBuffer, escapeSequencePos, strBuffer, size);
            }
            finally
            {
                if (allocated != null) //precaution
                    _context.ReturnMemory(allocated);
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

                WriteEscapeCharacter(_buffer, b, ref _pos);
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
            WriteRawString(buffer, size);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteRawString(byte* buffer, int size)
        {
            if (size < JsonOperationContext.ManagedPinnedBuffer.Size)
            {
                EnsureBuffer(size);
                Memory.Copy(_buffer + _pos, buffer, size);
                _pos += size;
                return;
            }

            UnlikelyWriteLargeRawString(buffer, size);
        }

        private void UnlikelyWriteLargeRawString(byte* buffer, int size)
        {
            // need to do this in pieces
            var posInStr = 0;
            while (posInStr < size)
            {
                var amountToCopy = Math.Min(size - posInStr, JsonOperationContext.ManagedPinnedBuffer.Size);
                Flush();
                Memory.Copy(_buffer, buffer + posInStr, amountToCopy);
                posInStr += amountToCopy;
                _pos = amountToCopy;
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
        private void EnsureBuffer(int len)
        {
            if (len >= JsonOperationContext.ManagedPinnedBuffer.Size)
                ThrowValueTooBigForBuffer();
            if (_pos + len < JsonOperationContext.ManagedPinnedBuffer.Size)
                return;

            Flush();
        }

        public void Flush()
        {
            if (_stream == null)
                ThrowStreamClosed();
            if (_pos == 0)
                return;
            _stream.Write(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pos);
            _pos = 0;
        }

        private static void ThrowValueTooBigForBuffer()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len");
        }

        private void ThrowStreamClosed()
        {
            throw new ObjectDisposedException("The stream was closed already.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNull()
        {
            EnsureBuffer(4);
            for (int i = 0; i < 4; i++)
            {
                _buffer[_pos++] = NullBuffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBool(bool val)
        {
            EnsureBuffer(5);
            var buffer = val ? TrueBuffer : FalseBuffer;
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer[_pos++] = buffer[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteComma()
        {
            EnsureBuffer(1);
            _buffer[_pos++] = Comma;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(LazyStringValue prop)
        {
            WriteString(prop);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(string prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePropertyName(StringSegment prop)
        {
            var lazyProp = _context.GetLazyStringForFieldWithCaching(prop);
            WriteString(lazyProp);
            EnsureBuffer(1);
            _buffer[_pos++] = Colon;
        }

        public void WriteInteger(long val)
        {
            if (val == 0)
            {
                EnsureBuffer(1);
                _buffer[_pos++] = (byte)'0';
                return;
            }

            var localBuffer = _parserAuxiliarMemory.Address;

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
            }
            while (val != 0);

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

        public void WriteBufferFor(byte[] buffer)
        {
            EnsureBuffer(buffer.Length);
            for (int i = 0; i < buffer.Length; i++)
            {
                _buffer[_pos++] = buffer[i];
            }
        }

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

            using (var lazyStr = _context.GetLazyString(val.ToString(CultureInfo.InvariantCulture)))
            {
                EnsureBuffer(lazyStr.Size);
                WriteRawString(lazyStr.Buffer, lazyStr.Size);
            }
        }

        public virtual void Dispose()
        {
            try
            {
                Flush();
            }
            catch (ObjectDisposedException)
            {
                //we are disposing, so this exception doesn't matter
            }
            finally
            {
                _returnBuffer.Dispose();
                _context.ReturnMemory(_parserAuxiliarMemory);
            }
        }

        public void WriteNewLine()
        {
            EnsureBuffer(2);
            _buffer[_pos++] = (byte)'\r';
            _buffer[_pos++] = (byte)'\n';
        }

        public void WriteStream(Stream stream)
        {
            Flush();

            while (true)
            {
                _pos = stream.Read(_pinnedBuffer.Buffer.Array, _pinnedBuffer.Buffer.Offset, _pinnedBuffer.Buffer.Count);
                if (_pos == 0)
                    break;

                Flush();
            }
        }

        public void WriteMemoryChunk(IntPtr ptr, int size)
        {
            Flush();
            var p = (byte*)ptr.ToPointer();
            var leftToWrite = size;
            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(JsonOperationContext.ManagedPinnedBuffer.Size, leftToWrite);
                Memory.Copy(_buffer, p + totalWritten, toWrite);
                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                Flush();
            }
        }
    }
}
