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
        private readonly Stream _stream;
        private readonly byte[] _buffer;
        private char[] _charBuffer;
        private byte[] _smallBuffer;
        private int _pos;
        private int _bufSize;
        public Tokens Current;
        private readonly UnmanagedWriteBuffer _strBuffer;
        private GCHandle _bufferHandle;
        private readonly byte* _bufferPtr;

        private int _line = 1;
        private int _charPos = 1;

        public readonly List<int> EscapePositions = new List<int>(); 

        public long Long;
        public double Double;

        public UnmanagedJsonParser(Stream stream, RavenOperationContext ctx)
        {
            _stream = stream;
            _buffer = ctx.GetManagedBuffer();
            _bufferHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            try
            {
                _bufferPtr = (byte*)_bufferHandle.AddrOfPinnedObject();
                _strBuffer = new UnmanagedWriteBuffer(ctx.Pool);
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
            EnsureBuffer(0);
            var b = _buffer[_pos++];
            _charPos++;
            switch (b)
            {
                case (byte)'\r':
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

                case (byte)',':
                    switch (Current)
                    {
                        case Tokens.Comma:
                        case Tokens.StartObject:
                        case Tokens.StartArray:
                            throw CreateException("Cannot have a comma in this position");
                    }
                    Current = Tokens.Comma;
                    break;
                case (byte)'N':
                    EnsureRestOfToken(NaN, "NaN");
                    Current = Tokens.NaN;
                    _pos += 2;
                    _charPos += 2;
                    break;
                case (byte)'n':
                    EnsureRestOfToken(BlittableJsonTextWriter.NullBuffer, "null");
                    Current = Tokens.Null;
                    _pos += 3;
                    _charPos += 3;
                    break;
                case (byte)'t':
                    EnsureRestOfToken(BlittableJsonTextWriter.TrueBuffer, "true");
                    Current = Tokens.True;
                    _pos += 3;
                    _charPos += 3;
                    break;
                case (byte)'f':
                    EnsureRestOfToken(BlittableJsonTextWriter.FalseBuffer, "false");
                    Current = Tokens.False;
                    _pos += 4;
                    _charPos += 4;
                    break;
                case (byte)'"':
                case (byte)'\'':
                    ParseString(b);
                    break;
                case (byte)'{':
                    Current = Tokens.StartObject;
                    break;
                case (byte)'[':
                    Current = Tokens.StartArray;
                    break;
                case (byte)'}':
                    Current = Tokens.EndObject;
                    break;
                case (byte)']':
                    Current = Tokens.EndArray;
                    break;
                //numbers

                //case (byte)'0':// we don't support numbers starting with 0 (0x for hex or 0 for octal)
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
                    break;
            }
        }

        private void ParseNumber(byte b)
        {
            if (_smallBuffer == null)
                _smallBuffer = new byte[32];//max decimal size in chars is 29, max double is 23

            var numLen = 0;
            var isDouble = false;
            var isExponent = false;
            do
            {
                switch (b)
                {
                    case (byte)'.':
                        if (isDouble)
                            throw CreateException("Already got '.' in this number value");
                        isDouble = true;
                        break;
                    case (byte)'e':
                    case (byte)'E':
                        if (isExponent)
                            throw CreateException("Already got 'e' in this number value");
                        isExponent = true;
                        isDouble = true;
                        break;
                    case (byte)'-':
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
                                if (isDouble)
                                {
                                    Current = Tokens.Double;
                                    var s = Encoding.UTF8.GetString(_smallBuffer, 0, numLen);
                                    if (double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands,
                                        CultureInfo.InvariantCulture, out Double))
                                        return;
                                    throw CreateException("Value is not a valid number");
                                }
                                Current = Tokens.Number;
                                Long = 0;
                                var i = _smallBuffer[0] == (byte)'-' ? 1 : 0;
                                for (; i < numLen; i++)
                                {
                                    Long *= 10;
                                    Long += '0' - _smallBuffer[i];
                                }
                                if (_smallBuffer[0] == (byte)'-')
                                {
                                    if (numLen == 1)
                                        throw CreateException("Number cannot be a single minus sign");
                                    Long ^= -1;//switch the sign
                                }
                                return;
                            default:
                                throw CreateException("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
                        }
                }
                _smallBuffer[numLen++] = b;
                EnsureBuffer(1);
                b = _buffer[_pos++];
                _charPos++;
            } while (true);
        }

        private void ParseString(byte quote)
        {
            EscapePositions.Clear();
            _strBuffer.Clear();
            while (true)
            {
                var start = _pos;
                while (_pos < _bufSize)
                {
                    var b = _buffer[_pos++];
                    if (b == quote)
                    {
                        Current = Tokens.String;
                        return;
                    }
                    if (b == (byte)'\\')
                    {
                        _strBuffer.Write(_bufferPtr + start, _pos - start);
                        start = _pos;
                        EnsureBuffer(1);

                        b = _buffer[_pos++];

                        if (b != (byte) 'u')
                            EscapePositions.Add(_strBuffer.SizeInBytes);

                        switch (b)
                        {
                            case (byte)'r':
                                _strBuffer.WriteByte((byte)'\r');
                                break;
                            case (byte)'n':
                                _strBuffer.WriteByte((byte)'\n');
                                break;
                            case (byte)'b':
                                _strBuffer.WriteByte((byte)'\b');
                                break;
                            case (byte)'f':
                                _strBuffer.WriteByte((byte)'\f');
                                break;
                            case (byte)'t':
                                _strBuffer.WriteByte((byte)'\t');
                                break;
                            case (byte)'"':
                            case (byte)'\\':
                            case (byte)'/':
                                _strBuffer.WriteByte(b);
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
                                break;
                            default:
                                throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
                        }
                        
                    }
                }
                // copy the buffer to the native code, then refill
                _strBuffer.Write(_bufferPtr + start, _pos - start);
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
                _strBuffer.Write(p, byteCount);
            _pos += 4;
        }

        public enum Tokens
        {
            Null,
            False,
            True,
            String,
            Double,
            Number,
            NaN,
            Comma,
            StartObject,
            StartArray,
            EndArray,
            EndObject
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer(int minRead)
        {
            if (_pos + minRead < _bufSize)
                return;
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
            _strBuffer?.Dispose();
        }
    }
}