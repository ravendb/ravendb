using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.CodeAnalysis.CSharp.Syntax;

using Raven.Abstractions.Indexing;
using Sparrow;
using Voron.Exceptions;

namespace Raven.Server.Json
{
    public unsafe class BlittableJsonTextWriter
    {
        private readonly Stream _stream;

        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        public static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        public static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        public static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private int _pos;
        private readonly byte[] _buffer;

        public BlittableJsonTextWriter(RavenOperationContext context, Stream stream)
        {
            _stream = stream;
            _buffer = context.GetManagedBuffer();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyStringValue str)
        {
            var strBuffer = str.Buffer;
            var size = str.Size;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = size;
            var numberOfEscapeSequences = ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);
                EnsureBuffer(2);
                _buffer[_pos++] = (byte)'\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }

        private byte GetEscapeCharacter(byte b)
        {
            switch (b)
            {
                case (byte) '\b':
                     return (byte) 'b';
                case (byte) '\t':
                    return (byte) 't';
                case (byte) '\n':
                    return (byte) 'n';
                case (byte) '\f':
                    return (byte) 'f';
                case (byte) '\r':
                    return (byte) 'r';
                case (byte) '\\':
                    return (byte) '\\';
                case (byte)'/':
                    return (byte)'/';
                case (byte) '"':
                    return (byte) '"';
                default:
                    throw new InvalidOperationException("Invalid escape char '"+(char)b +"' numeric value is: " + b);
            }
        }

        public static int ReadVariableSizeInt(byte* buffer ,ref int pos)
        {
            // ReadAsync out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                b = buffer[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyCompressedStringValue str)
        {
            var strBuffer = str.DecompressToTempBuffer();

            var size = str.UncompressedSize;

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
            var escapeSequencePos = str.CompressedSize;
            var numberOfEscapeSequences = ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = ReadVariableSizeInt(str.Buffer, ref escapeSequencePos);
                WriteRawString(strBuffer, bytesToSkip);
                strBuffer += bytesToSkip;
                size -= bytesToSkip + 1 /*for the escaped char we skip*/;
                var b = *(strBuffer++);
                EnsureBuffer(2);
                _buffer[_pos++] = (byte)'\\';
                _buffer[_pos++] = GetEscapeCharacter(b);
            }
            // write remaining (or full string) to the buffer in one shot
            WriteRawString(strBuffer, size);

            EnsureBuffer(1);
            _buffer[_pos++] = Quote;
        }


        private void WriteRawString(byte* buffer, int size)
        {
            if (size < _buffer.Length)
            {
                EnsureBuffer(size);
                fixed (byte* p = _buffer)
                    Memory.Copy(p + _pos, buffer, size);
                _pos += size;
                return;
            }
            
            // need to do this in pieces
            var posInStr = 0;
            fixed (byte* p = _buffer)
            {
                while (posInStr < size)
                {
                    var amountToCopy = Math.Min(size - posInStr, _buffer.Length);
                    Flush();
                    Memory.Copy(p, buffer + posInStr, amountToCopy);
                    posInStr += amountToCopy;
                    _pos = amountToCopy;
                }
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
            if (_pos + len < _buffer.Length)
                return;
            if (len >= _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(len));

            Flush();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            if (_pos == 0)
                return;
            _stream.Write(_buffer, 0, _pos);
            _pos = 0;
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

        public void WriteInteger(long val)
        {
            if (val == 0) 
            {
                EnsureBuffer(1);
                _buffer[_pos++] = (byte)'0';
                return;
            }
            int len = 1;
           
            for (var i = val / 10; i != 0; i /= 10)
            {
                len++;
            }
            if (val < 0)
            {
                EnsureBuffer(len + 1);
                _buffer[_pos++] = (byte) '-';
            }
            else
            {
                EnsureBuffer(len);
            }
            for (int i = len - 1; i >= 0; i--)
            {
                _buffer[_pos + i] = (byte)('0' + Math.Abs(val % 10));
                val /= 10;
            }
            _pos += len;
        }

        public void WriteDouble(LazyDoubleValue val)
        {
            var lazyStringValue = val.Inner;
            WriteRawString(lazyStringValue.Buffer,lazyStringValue.Size);
        }
    }


}