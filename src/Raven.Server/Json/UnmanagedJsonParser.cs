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
        private int _currentStrStart;

        public void Read()
        {
            if (_line == 0)
            {
                // first time, need to check preamble
                _line++;
                LoadBufferFromStream();
                if (_buffer[_pos] == Utf8Preamble[0])
                {
                    _pos++;
                    EnsureRestOfToken(Utf8Preamble, "UTF8 Preamble");
                }
            }

            while (true)
            {
                EnsureBuffer();
                var b = _buffer[_pos++];
                _charPos++;
                switch (b)
                {
                    case (byte)'\r':
                        EnsureBuffer();
                        if (_buffer[_pos] == (byte)'\n')
                            continue;
                        goto case (byte)'\n';
                    case (byte)'\n':
                        _line++;
                        _charPos = 1;
                        break;
                    case (byte)' ':
                    case (byte)'\t':
                    case (byte)'\v':
                    case (byte)'\f':
                        //white space, we can safely ignore
                        break;
                    case (byte)':':
                    case (byte)',':
                        switch (Current)
                        {
                            case Tokens.Separator:
                            case Tokens.StartObject:
                            case Tokens.StartArray:
                                throw CreateException("Cannot have a '" + (char)b + "' in this position");
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
                    case (byte)'1':
                    case (byte)'2':
                    case (byte)'3':
                    case (byte)'4':
                    case (byte)'5':
                    case (byte)'6':
                    case (byte)'7':
                    case (byte)'8':
                    case (byte)'9':
                    case (byte)'-':// negative number
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
                        if (isNegative)
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
                        Long += b - (byte)'0';
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
                            case (byte)']':
                            case (byte)'}':
                                if (zeroPrefix && StringBuffer.SizeInBytes != 1)
                                    throw CreateException("Invalid number with zero prefix");
                                if (isNegative)
                                    Long *= -1;
                                Current = isDouble ? Tokens.Float : Tokens.Integer;
                                _pos--; _charPos--;// need to re-read this char
                                return;
                            default:
                                throw CreateException("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
                        }
                }
                StringBuffer.WriteByte(b);
                EnsureBuffer();
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
                _currentStrStart = _pos;
                while (_pos < _bufSize)
                {
                    var b = _buffer[_pos++];
                    _charPos++;
                    if (b == quote)
                    {
                        Current = Tokens.String;
                        StringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart - 1 /*don't include the last quote*/);
                        return;
                    }
                    if (b == (byte)'\\')
                    {
                        StringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart - 1);
                        
                        EnsureBuffer();

                        b = _buffer[_pos++];
                        _currentStrStart = _pos;
                        _charPos++;
                        if (b != (byte)'u')
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
                                EnsureBuffer();// flush the buffer, but skip the \,\r chars
                                _line++;
                                _charPos = 1;
                                EnsureBuffer();
                                if (_buffer[_pos] == (byte)'\n')
                                    _pos++; // consume the \,\r,\n
                                break;
                            case (byte)'\n':
                                _line++;
                                _charPos = 1;
                                break;// line continuation, skip
                            case (byte)'u':// unicode value
                                ParseUnicodeValue();
                                _currentStrStart += 4;
                                break;
                            default:
                                throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
                        }

                    }
                }
                // copy the buffer to the native code, then refill
                StringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart);
                EnsureBuffer();
            }
        }

        private void ParseUnicodeValue()
        {
            byte b;
            int val = 0;
            for (int i = 0; i < 4; i++)
            {
                EnsureBuffer();
            
                b = _buffer[_pos++];
                if (b >= (byte)'0' && b <= (byte)'9')
                {
                    val = (val << 4) | ( b- (byte)'0');
                }
                else if (b >= 'a' && b <= (byte)'f')
                {
                    val = (val << 4) | (10+(b- (byte)'a'));
                }
                else if (b >= 'A' && b <= (byte)'F')
                {
                    val = (val << 4) | (10 + (b - (byte) 'A'));
                }
                else
                {
                    throw CreateException("Invalid hex value , numeric value is: " + b);
                }
            }
            var chars = stackalloc char[1];
            try
            {
                chars[0] = Convert.ToChar(val);
            }
            catch (Exception e)
            {
                throw new FormatException("Could not convert value " + val + " to char", e);
            }
            var smallBuffer = stackalloc byte[8];
            var byteCount = Encoding.UTF8.GetBytes(chars, 1, smallBuffer, 8);
            StringBuffer.Write(smallBuffer, byteCount);
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
        private void EnsureBuffer()
        {
            if (_pos >= _bufSize)
                LoadBufferFromStream();
        }


        private void LoadBufferFromStream()
        {
            _currentStrStart = 0;
            _pos = 0;
            _bufSize = 0;
            var read = _stream.Read(_buffer, _bufSize, _buffer.Length - _bufSize);
            if (read == 0)
                throw new EndOfStreamException();
            _bufSize += read;
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
                _pos = 0;
            }
            for (int i = 0; i < size; i++)
            {
                if (_buffer[_pos++] != buffer[i + 1])
                    throw CreateException("Invalid token found, expected: " + expected);
            }
        }

        private InvalidDataException CreateException(string message)
        {
            var start = Math.Max(0, _pos - 25);
            var count = Math.Min(_pos, _buffer.Length) - start;
            var s = Encoding.UTF8.GetString(_buffer, start, count);
            return new InvalidDataException(message + " at (" + _line + "," + _charPos + ") around: " + s );
        }

        public void Dispose()
        {
            _bufferHandle.Free();
            StringBuffer?.Dispose();
        }
    }
}