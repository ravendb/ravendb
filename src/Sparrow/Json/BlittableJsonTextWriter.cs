using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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
        public static ReadOnlySpan<byte> NaNBuffer => new byte[] { (byte)'"', (byte)'N', (byte)'a', (byte)'N', (byte)'"' };
        public static ReadOnlySpan<byte> PositiveInfinityBuffer => new byte[]
        {
            (byte)'"', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };        

        public static ReadOnlySpan<byte> NegativeInfinityBuffer => new byte[]
        {
            (byte)'"', (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y', (byte)'"'
        };
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private static ReadOnlySpan<byte> EscapeCharacters => InitializeEscapeCharacters();

        public static ReadOnlySpan<int> ControlCodeEscapes => InitializeControlCodeEscapes();

        private static ReadOnlySpan<int> InitializeControlCodeEscapes()
        {
            var escapes = new int[32];

            for (int i = 0; i < 32; i++)
            {
                byte[] bytes = Encodings.Utf8.GetBytes(i.ToString("X4"));
                escapes[i] = (int)bytes[0] << 24 | (int)bytes[1] << 16 | (int)bytes[2] << 8 | (int)bytes[3];
            }

            return new ReadOnlySpan<int>(escapes);
        }

        private int _pos;
        private readonly byte* _buffer;
        private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
        private readonly JsonOperationContext.MemoryBuffer _pinnedBuffer;
        private readonly AllocatedMemoryData _parserAuxiliarMemory;

        private static ReadOnlySpan<byte> InitializeEscapeCharacters()
        {
            var escapeCharacters = new byte[256];
            for (int i = 0; i < 32; i++)
                escapeCharacters[i] = 0;

            for (int i = 32; i < escapeCharacters.Length; i++)
                escapeCharacters[i] = 255;

            escapeCharacters[(byte)'\b'] = (byte)'b';
            escapeCharacters[(byte)'\t'] = (byte)'t';
            escapeCharacters[(byte)'\n'] = (byte)'n';
            escapeCharacters[(byte)'\f'] = (byte)'f';
            escapeCharacters[(byte)'\r'] = (byte)'r';
            escapeCharacters[(byte)'\\'] = (byte)'\\';
            escapeCharacters[(byte)'/'] = (byte)'/';
            escapeCharacters[(byte)'"'] = (byte)'"';

            return escapeCharacters;
        }

        protected AbstractBlittableJsonTextWriter(JsonOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;

            _returnBuffer = context.GetMemoryBuffer(out _pinnedBuffer);
            _buffer = _pinnedBuffer.Pointer;

            _parserAuxiliarMemory = context.GetMemory(32);
        }

        public int Position => _pos;

        public override string ToString()
        {
            return Encodings.Utf8.GetString(_pinnedBuffer.Memory.Span.Slice(0, _pos));
        }

        public void WriteObject(BlittableJsonReaderObject obj)
        {
            if (obj == null)
            {
                WriteNull();
                return;
            }

            WriteStartObject();
            
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffer = obj.GetPropertiesByInsertionOrder())
            {
                for (int i = 0; i < buffer.Size; i++)
                {
                    if (i != 0)
                    {
                        WriteComma();
                    }

                    obj.GetPropertyByIndex(buffer.Properties[i], ref prop);
                    WritePropertyName(prop.Name);

                    WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
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
                case BlittableJsonToken.RawBlob:
                    var blob = (BlittableJsonReaderObject.RawBlob)val;
                    WriteRawString(blob.Ptr, blob.Length);
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
            if (bufferSize >= JsonOperationContext.MemoryBuffer.Size)
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
            byte r = EscapeCharacters[b];
            if (r == 0)
            {
                EnsureBuffer(6);

                int controlCodes = ControlCodeEscapes[b];

                buffer[_pos + 0] = (byte)'\\';
                buffer[_pos + 1] = (byte)'u';
                buffer[_pos + 2] = (byte)(controlCodes >> 24);
                buffer[_pos + 3] = (byte)(controlCodes >> 16);
                buffer[_pos + 4] = (byte)(controlCodes >> 8);
                buffer[_pos + 5] = (byte)(controlCodes);

                _pos += 6;
                return;
            }

            if (r != 255)
            {
                EnsureBuffer(2);
                buffer[_pos + 0] = (byte)'\\';
                buffer[_pos + 1] = r;
                _pos += 2;
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
                if (bufferSize >= JsonOperationContext.MemoryBuffer.Size)
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

                    WriteEscapeCharacter(_buffer, b);
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
            WriteRawString(buffer, size);
            _buffer[_pos++] = Quote;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteRawString(byte* buffer, int size)
        {
            if (size < JsonOperationContext.MemoryBuffer.Size)
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
                var amountToCopy = Math.Min(size - posInStr, JsonOperationContext.MemoryBuffer.Size);
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
            if (len >= JsonOperationContext.MemoryBuffer.Size)
                ThrowValueTooBigForBuffer(len);
            if (_pos + len < JsonOperationContext.MemoryBuffer.Size)
                return;

            Flush();
        }

        public void Flush()
        {
            if (_stream == null)
                ThrowStreamClosed();
            if (_pos == 0)
                return;
            _stream.Write(_pinnedBuffer.Memory.Span.Slice(0, _pos));
            _pos = 0;
        }

        private static void ThrowValueTooBigForBuffer(int len)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("len", len, "Length value too big: " + len);
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

        public void WriteBufferFor(ReadOnlySpan<byte> buffer)
        {
            EnsureBuffer(buffer.Length);

            var outputSpan = new Span<byte>(_buffer + _pos, buffer.Length);
            buffer.CopyTo(outputSpan);
            _pos += buffer.Length;
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
            // TODO: remove when we update to .net core 3
            // https://github.com/dotnet/corefx/issues/36141
            catch (NotSupportedException e)
            {
                throw new IOException("The stream was closed by the peer.", e);
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
                _pos = stream.Read(_pinnedBuffer.Memory.Span);
                if (_pos == 0)
                    break;

                Flush();
            }
        }

        public void WriteMemoryChunk(IntPtr ptr, int size)
        {
            WriteMemoryChunk((byte*)ptr.ToPointer(), size);
        }

        public void WriteMemoryChunk(byte* ptr, int size)
        {
            Flush();
            var leftToWrite = size;
            var totalWritten = 0;
            while (leftToWrite > 0)
            {
                var toWrite = Math.Min(JsonOperationContext.MemoryBuffer.Size, leftToWrite);
                Memory.Copy(_buffer, ptr + totalWritten, toWrite);
                _pos += toWrite;
                totalWritten += toWrite;
                leftToWrite -= toWrite;
                Flush();
            }
        }
    }
}
