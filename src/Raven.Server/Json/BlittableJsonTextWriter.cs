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
        private readonly RavenOperationContext _context;
        private readonly Stream _stream;

        private const byte StartObject = (byte)'{';
        private const byte EndObject = (byte)'}';
        private const byte StartArray = (byte)'[';
        private const byte EndArray = (byte)']';
        private const byte Comma = (byte)',';
        private const byte Quote = (byte)'"';
        private const byte Colon = (byte)':';
        private static readonly byte[] NullBuffer = { (byte)'n', (byte)'u', (byte)'l', (byte)'l', };
        private static readonly byte[] TrueBuffer = { (byte)'t', (byte)'r', (byte)'u', (byte)'e', };
        private static readonly byte[] FalseBuffer = { (byte)'f', (byte)'a', (byte)'l', (byte)'s', (byte)'e', };

        private int _pos;
        private readonly byte[] _buffer;

        public BlittableJsonTextWriter(RavenOperationContext context, Stream stream)
        {
            _context = context;
            _stream = stream;
            _buffer = context.GetManagedBuffer();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyStringValue str)
        {
            var buffer = str.Buffer;
            var size = str.Size;

            WriteString(buffer, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(LazyCompressedStringValue str)
        {
            var buffer = str.DecompressToTempBuffer();

            WriteString(buffer, str.UncompressedSize);
        }

        private void WriteString(byte* buffer, int size)
        {
            EnsureBuffer(1);
            _buffer[_pos++] = Quote;

            WriteRawString(buffer, size);

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
                int amountToCopy = _pos;
                while (posInStr < size)
                {
                    amountToCopy = Math.Min(size - posInStr, _buffer.Length);
                    Flush();
                    Memory.Copy(p, buffer + posInStr, amountToCopy);
                    posInStr += amountToCopy;
                }
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
            if (len < 0)
            {
                EnsureBuffer(1);
                _buffer[_pos++] = (byte)'-';
            }
            for (var i = val / 10; i != 0; i /= 10)
            {
                len++;
            }
            EnsureBuffer(len);
            for (int i = len - 1; i >= 0; i--)
            {
                _buffer[_pos + i] = (byte)('0' + (val % 10));
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