using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Json
{
    public unsafe class UnmanagedJsonParser : IDisposable
    {
        public static readonly byte[] Utf8Preamble = System.Text.Encoding.UTF8.GetPreamble();

        private readonly Stream _stream;
        private readonly byte[] _buffer;
        private char[] _charBuffer;
        private byte[] _smallBuffer;
        private int _pos;
        private int _bufSize;
        public Tokens Current;
        public readonly UnmanagedWriteBuffer StringBuffer;
        private GCHandle _bufferHandle;
        private readonly byte* _bufferPtr;

        private int _line;
        private int _charPos = 1;

        public readonly List<int> EscapePositions = new List<int>(); 

        public long Long;

        public UnmanagedJsonParser(Stream stream, RavenOperationContext ctx)
        {
            _stream = stream;
            _buffer = ctx.GetManagedBuffer();
            _bufferHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            try
            {
                _bufferPtr = (byte*)_bufferHandle.AddrOfPinnedObject();
                StringBuffer = new UnmanagedWriteBuffer(ctx.Pool);
            }
            catch (Exception)
            {
                _bufferHandle.Free();
                throw;
            }
        }

        private static readonly byte[] NaN = { (byte)'N', (byte)'a', (byte)'N' };

        public void Read()
        {
            if (_line == 0)
            {
                // first time, need to check preamble
                _line++;
                EnsureBuffer(1);
                if (_buffer[_pos] == Utf8Preamble[0])
                {
                    _pos++;
                    EnsureRestOfToken(Utf8Preamble, "UTF8 Preamble");
                }
            }

            while (true)
            {
                EnsureBuffer(0);
                var b = _buffer[_pos++];
                _charPos++;
                switch (b)
                {
                    case (byte)'\r':
                        EnsureBuffer(0);
                        if(_buffer[_pos]== (byte)'\n')
                            continue;
                        goto case (byte) '\n';
                    case (byte)'\n':
                        _line++;
                        _charPos = 1;
                        break;
                    case (byte)' ': case (byte)'\t': case (byte)'\v': case (byte)'\f':
                        //white space, we can safely ignore
                        break;
                    case (byte)':':
                    case (byte)',':
                        switch (Current)
                        {
                            case Tokens.Separator:
                            case Tokens.StartObject:
                            case Tokens.StartArray:
                                throw CreateException("Cannot have a '"+(char)b +"' in this position");
                        }
                        Current = Tokens.Separator;
                        break;
                    case (byte)'N':
                        EnsureRestOfToken(NaN, "NaN");
                        Current = Tokens.Float;
                        _charPos += 2;
                        return;
                    case (byte)'n':
                        EnsureRestOfToken(BlittableJsonTextWriter.NullBuffer, "null");
                        Current = Tokens.Null;
                        _charPos += 3;
                        return;
                    case (byte)'t':
                        EnsureRestOfToken(BlittableJsonTextWriter.TrueBuffer, "true");
                        Current = Tokens.True;
                        _charPos += 3;
                        return;
                    case (byte)'f':
                        EnsureRestOfToken(BlittableJsonTextWriter.FalseBuffer, "false");
                        Current = Tokens.False;
                        _charPos += 4;
                        return;
                    case (byte)'"':
                    case (byte)'\'':
                        ParseString(b);
                        return;
                    case (byte)'{':
                        Current = Tokens.StartObject;
                        return;
                    case (byte)'[':
                        Current = Tokens.StartArray;
                        return;
                    case (byte)'}':
                        Current = Tokens.EndObject;
                        return;
                    case (byte)']':
                        Current = Tokens.EndArray;
                        return;
                    //numbers

                    case (byte)'0':
                    case (byte)'1':case (byte)'2':case (byte)'3':case (byte)'4':case (byte)'5':
                    case (byte)'6':case (byte)'7':case (byte)'8':case (byte)'9':case (byte)'-':// negative number
                        ParseNumber(b);
                        return;
                }
            }
        }

        private void ParseNumber(byte b)
        {
            StringBuffer.Clear();
            EscapePositions.Clear();
            Long = 0;

            var zeroPrefix = b == '0';
            var isNegative = false;
            var isDouble = false;
            var isExponent = false;
            do
            {
                switch (b)
                {
                    case (byte)'.':
                        if (isDouble)
                            throw CreateException("Already got '.' in this number value");
                        zeroPrefix = false; // 0.5, frex
                        isDouble = true;
                        break;
                    case (byte)'+':
                        break; // just record, appears in 1.4e+3
                    case (byte)'e':
                    case (byte)'E':
                        if (isExponent)
                            throw CreateException("Already got 'e' in this number value");
                        isExponent = true;
                        isDouble = true;
                        break;
                    case (byte)'-':
                        if(isNegative)
                            throw CreateException("Already got '-' in this number value");
                        isNegative = true;
                        break;
                    case (byte)'0':
                    case (byte)'1':
                    case (byte)'2':
                    case (byte)'3':
                    case (byte)'4':
                    case (byte)'5':
                    case (byte)'6':
                    case (byte)'7':
                    case (byte)'8':
                    case (byte)'9':
                        Long *= 10;
                        Long += b- (byte)'0';
                        break;
                    default:
                        switch (b)
                        {
                            case (byte)'\r':
                            case (byte)'\n':
                                _line++;
                                _charPos = 1;
                                goto case (byte)' ';
                            case (byte)' ':
                            case (byte)'\t':
                            case (byte)'\v':
                            case (byte)'\f':
                            case (byte)',':
                                if (zeroPrefix && StringBuffer.SizeInBytes != 1)
                                    throw CreateException("Invalid number with zero prefix");
                                if (isNegative)
                                    Long *= -1;
                                Current = isDouble ? Tokens.Float : Tokens.Integer;
                                return;
                            default:
                                throw CreateException("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
                        }
                }
                StringBuffer.WriteByte(b);
                EnsureBuffer(1);
                b = _buffer[_pos++];
                _charPos++;
            } while (true);
        }

        private void ParseString(byte quote)
        {
            EscapePositions.Clear();
            StringBuffer.Clear();
            while (true)
            {
                var start = _pos;
                while (_pos < _bufSize)
                {
                    var b = _buffer[_pos++];
                    if (b == quote)
                    {
                        Current = Tokens.String;
                        StringBuffer.Write(_bufferPtr + start, _pos - start-1 /*don't include the last quote*/);
                        return;
                    }
                    if (b == (byte)'\\')
                    {
                        StringBuffer.Write(_bufferPtr + start, _pos - start-1);
                        start = _pos + 1;
                        EnsureBuffer(1);

                        b = _buffer[_pos++];

                        if (b != (byte) 'u')
                            EscapePositions.Add(StringBuffer.SizeInBytes);

                        switch (b)
                        {
                            case (byte)'r':
                                StringBuffer.WriteByte((byte)'\r');
                                break;
                            case (byte)'n':
                                StringBuffer.WriteByte((byte)'\n');
                                break;
                            case (byte)'b':
                                StringBuffer.WriteByte((byte)'\b');
                                break;
                            case (byte)'f':
                                StringBuffer.WriteByte((byte)'\f');
                                break;
                            case (byte)'t':
                                StringBuffer.WriteByte((byte)'\t');
                                break;
                            case (byte)'"':
                            case (byte)'\\':
                            case (byte)'/':
                                StringBuffer.WriteByte(b);
                                break;
                            case (byte)'\r':// line continuation, skip
                                EnsureBuffer(1);// flush the buffer, but skip the \,\r chars
                                _line++;
                                _charPos = 1;
                                if (_buffer[_pos] == (byte)'\n')
                                    _pos++; // consume the \,\r,\n
                                break;
                            case (byte)'\n':
                                _line++;
                                _charPos = 1;
                                break;// line continuation, skip
                            case (byte)'u':// unicode value
                                ParseUnicodeValue();
                                start += 4;
                                break;
                            default:
                                throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
                        }
                        
                    }
                }
                // copy the buffer to the native code, then refill
                StringBuffer.Write(_bufferPtr + start, _pos - start);
                EnsureBuffer(1);
            }
        }

        private void ParseUnicodeValue()
        {
            byte b;
            EnsureBuffer(4); // flush the buffer, but skip the \,u chars
            int val = 0;
            for (int i = 0; i < 4; i++)
            {
                b = _buffer[_pos + i];
                if (b >= (byte)'0' && b <= (byte)'9')
                {
                    val = (val << 4) | ((byte)'0' - b);
                }
                else if (b >= 'a' && b <= (byte)'f')
                {
                    val = (val << 4) | ((byte)'a' - b);
                }
                else if (b >= 'A' && b <= (byte)'F')
                {
                    val = (val << 4) | ((byte)'A' - b);
                }
                else
                {
                    throw CreateException("Invalid hex value , numeric value is: " + b);
                }
            }
            if (_charBuffer == null)
                _charBuffer = new char[1];
            _charBuffer[0] = Convert.ToChar(val);
            if (_smallBuffer == null)
                _smallBuffer = new byte[32]; // more than big enough for any single utf8 character
            var byteCount = Encoding.UTF8.GetBytes(_charBuffer, 0, 1,
                _smallBuffer, 0);
            fixed (byte* p = _smallBuffer)
                StringBuffer.Write(p, byteCount);
            _pos += 4;
        }

        public enum Tokens
        {
            Null,
            False,
            True,
            String,
            Float,
            Integer,
            Separator,
            StartObject,
            StartArray,
            EndArray,
            EndObject
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer(int minRead)
        {
            if (_pos + minRead >= _bufSize)
                LoadBufferFromStream(minRead);
        }

        private void LoadBufferFromStream(int minRead)
        {
            _pos = 0;
            _bufSize = 0;
            do
            {
                var read = _stream.Read(_buffer, _bufSize, _buffer.Length - _bufSize);
                if (read == 0)
                    throw new EndOfStreamException();
                _bufSize += read;
                minRead -= read;
            } while (minRead > 0);
        }

        private void EnsureRestOfToken(byte[] buffer, string expected)
        {
            var size = buffer.Length - 1;
            while (_pos + size >= _bufSize)// end of buffer, need to read more bytes
            {
                var lenToMove = _bufSize - _pos;
                for (int i = 0; i < lenToMove; i++)
                {
                    _buffer[i] = _buffer[i + _pos];
                }
                _bufSize = _stream.Read(_buffer, lenToMove, _bufSize - lenToMove);
                if (_bufSize == 0)
                    throw new EndOfStreamException();
                _bufSize += lenToMove;
            }
            for (int i = 0; i < size; i++)
            {
                if (_buffer[_pos++] != buffer[i + 1])
                    throw CreateException("Invalid token found, expected: " + expected);
            }
        }

        private InvalidDataException CreateException(string message)
        {
            return new InvalidDataException(message + " at (" + _line + "," + _charPos + ") around: " +
                                            Encoding.UTF8.GetString(_buffer, Math.Max(0, _pos - 50), _pos)
                );
        }

        public void Dispose()
        {
            _bufferHandle.Free();
            StringBuffer?.Dispose();
        }
    }
}