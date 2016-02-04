using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Json.Parsing
{
    public abstract class UnmanagedJsonParserAbstract : UnmanagedJsonParserBase,IJsonParser
    {
        public static readonly byte[] Utf8Preamble = Encoding.UTF8.GetPreamble();
        private readonly UnmanagedWriteBuffer _stringBuffer;
        
        private readonly JsonParserState _state;
        protected readonly byte[] _buffer;
        protected int _pos;
        protected int _bufSize;
        private GCHandle _bufferHandle;
        
        private string _doubleStringBuffer;

        private int _line;
        private int _charPos = 1;



        public unsafe UnmanagedJsonParserAbstract(RavenOperationContext ctx, JsonParserState state, string documentId)
        {
            _state = state;
            _buffer = ctx.GetManagedBuffer();
            _bufferHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            try
            {
                _stringBuffer = ctx.GetStream(documentId);
                _bufferPtr = (byte*)_bufferHandle.AddrOfPinnedObject();
            }
            catch (Exception)
            {
                _stringBuffer?.Dispose();
                _bufferHandle.Free();
                throw;
            }
        }

        private static readonly byte[] NaN = { (byte)'N', (byte)'a', (byte)'N' };
        protected int _currentStrStart;

        public async Task ReadAsync()
        {
            if (_line == 0)
            {
                // first time, need to check preamble
                _line++;
                await LoadBufferFromSource();
                if (_buffer[_pos] == Utf8Preamble[0])
                {
                    _pos++;
                    await EnsureRestOfToken(Utf8Preamble, "UTF8 Preamble");
                }
            }

            while (true)
            {
                await EnsureBuffer();
                var b = _buffer[_pos++];
                _charPos++;
                switch (b)
                {
                    case (byte)'\r':
                        await EnsureBuffer();
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
                        switch (_state.CurrentTokenType)
                        {
                            case JsonParserToken.Separator:
                            case JsonParserToken.StartObject:
                            case JsonParserToken.StartArray:
                                throw CreateException("Cannot have a '" + (char)b + "' in this position");
                        }
                        _state.CurrentTokenType = JsonParserToken.Separator;
                        break;
                    case (byte)'N':
                        await EnsureRestOfToken(NaN, "NaN");
                        _state.CurrentTokenType = JsonParserToken.Float;
                        _charPos += 2;
                        return;
                    case (byte)'n':
                        await EnsureRestOfToken(BlittableJsonTextWriter.NullBuffer, "null");
                        _state.CurrentTokenType = JsonParserToken.Null;
                        _charPos += 3;
                        return;
                    case (byte)'t':
                        await EnsureRestOfToken(BlittableJsonTextWriter.TrueBuffer, "true");
                        _state.CurrentTokenType = JsonParserToken.True;
                        _charPos += 3;
                        return;
                    case (byte)'f':
                        await EnsureRestOfToken(BlittableJsonTextWriter.FalseBuffer, "false");
                        _state.CurrentTokenType = JsonParserToken.False;
                        _charPos += 4;
                        return;
                    case (byte)'"':
                    case (byte)'\'':
                        await ParseString(b);
                        _stringBuffer.EnsureSingleChunk(_state);
                        return;
                    case (byte)'{':
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        return;
                    case (byte)'[':
                        _state.CurrentTokenType = JsonParserToken.StartArray;
                        return;
                    case (byte)'}':
                        _state.CurrentTokenType = JsonParserToken.EndObject;
                        return;
                    case (byte)']':
                        _state.CurrentTokenType = JsonParserToken.EndArray;
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
                        await ParseNumber(b);
                        if (_state.CurrentTokenType == JsonParserToken.Float)
                        {
                            _stringBuffer.EnsureSingleChunk(_state);
                        }
                        return;
                }
            }
        }

        private async Task ParseNumber(byte b)
        {
            _stringBuffer.Clear();
            _state.EscapePositions.Clear();
            _state.Long = 0;

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
                        _state.Long *= 10;
                        _state.Long += b - (byte)'0';
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
                                if (zeroPrefix && _stringBuffer.SizeInBytes != 1)
                                    throw CreateException("Invalid number with zero prefix");
                                if (isNegative)
                                    _state.Long *= -1;
                                _state.CurrentTokenType = isDouble ? JsonParserToken.Float : JsonParserToken.Integer;
                                _pos--; _charPos--;// need to re-read this char
                                return;
                            default:
                                throw CreateException("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
                        }
                }
                _stringBuffer.WriteByte(b);
                await EnsureBuffer();
                b = _buffer[_pos++];
                _charPos++;
            } while (true);
        }


        public unsafe void ValidateFloat()
        {
            if (_doubleStringBuffer == null)
                _doubleStringBuffer = new string(' ', 25);
            if (_stringBuffer.SizeInBytes > 25)
                throw CreateException("Too many characters in double: " + _stringBuffer.SizeInBytes);

            var tmpBuff = stackalloc byte[_stringBuffer.SizeInBytes];
            // here we assume a clear char <- -> byte conversion, we only support
            // utf8, and those cleanly transfer
            fixed (char* pChars = _doubleStringBuffer)
            {
                int i = 0;
                _stringBuffer.CopyTo(tmpBuff);
                for (; i < _stringBuffer.SizeInBytes; i++)
                {
                    pChars[i] = (char)tmpBuff[i];
                }
                for (; i < _doubleStringBuffer.Length; i++)
                {
                    pChars[i] = ' ';
                }
            }
            try
            {
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                double.Parse(_doubleStringBuffer, NumberStyles.Any);
            }
            catch (Exception e)
            {
                throw CreateException("Could not parse double", e);
            }
        }

        private unsafe void WriteCurrentStringToStringBufferUnsafe(UnmanagedWriteBuffer stringBuffer, int bufferOffset, int positionOffset)
        {
            _stringBuffer.Write(_bufferPtr + _currentStrStart, _pos + positionOffset);
        }
        private async Task ParseString(byte quote)
        {
            _state.EscapePositions.Clear();
            _stringBuffer.Clear();
            var prevEscapePosition = 0;
            while (true)
            {
                _currentStrStart = _pos;
                while (_pos < _bufSize)
                {
                    var b = _buffer[_pos++];
                    _charPos++;
                    if (b == quote)
                    {
                        _state.CurrentTokenType = JsonParserToken.String;
                        WriteCurrentStringToStringBufferUnsafe(_stringBuffer, _currentStrStart,- _currentStrStart- 1 /*don't include the last quote*/);
                        //_stringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart - 1 /*don't include the last quote*/);
                        return;
                    }
                    if (b == (byte)'\\')
                    {
                        WriteCurrentStringToStringBufferUnsafe(_stringBuffer, _currentStrStart, -_currentStrStart - 1 /*don't include the last quote*/);
                        //_stringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart - 1);

                        await EnsureBuffer();

                        b = _buffer[_pos++];
                        _currentStrStart = _pos;
                        _charPos++;
                        if (b != (byte)'u')
                        {
                            _state.EscapePositions.Add(_stringBuffer.SizeInBytes - prevEscapePosition);
                            prevEscapePosition = _stringBuffer.SizeInBytes + 1;
                        }

                        switch (b)
                        {
                            case (byte)'r':
                                _stringBuffer.WriteByte((byte)'\r');
                                break;
                            case (byte)'n':
                                _stringBuffer.WriteByte((byte)'\n');
                                break;
                            case (byte)'b':
                                _stringBuffer.WriteByte((byte)'\b');
                                break;
                            case (byte)'f':
                                _stringBuffer.WriteByte((byte)'\f');
                                break;
                            case (byte)'t':
                                _stringBuffer.WriteByte((byte)'\t');
                                break;
                            case (byte)'"':
                            case (byte)'\\':
                            case (byte)'/':
                                _stringBuffer.WriteByte(b);
                                break;
                            case (byte)'\r':// line continuation, skip
                                await EnsureBuffer();// flush the buffer, but skip the \,\r chars
                                _line++;
                                _charPos = 1;
                                await EnsureBuffer();
                                if (_buffer[_pos] == (byte)'\n')
                                    _pos++; // consume the \,\r,\n
                                break;
                            case (byte)'\n':
                                _line++;
                                _charPos = 1;
                                break;// line continuation, skip
                            case (byte)'u':// unicode value
                                await ParseUnicodeValue();
                                _currentStrStart += 4;
                                break;
                            default:
                                throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
                        }

                    }
                }
                // copy the buffer to the native code, then refill
                WriteCurrentStringToStringBufferUnsafe(_stringBuffer, _currentStrStart, -_currentStrStart);
                //_stringBuffer.Write(_bufferPtr + _currentStrStart, _pos - _currentStrStart);
                await EnsureBuffer();
            }
        }

        private async Task ParseUnicodeValue()
        {
            byte b;
            int val = 0;
            for (int i = 0; i < 4; i++)
            {
                await EnsureBuffer();

                b = _buffer[_pos++];
                if (b >= (byte)'0' && b <= (byte)'9')
                {
                    val = (val << 4) | (b - (byte)'0');
                }
                else if (b >= 'a' && b <= (byte)'f')
                {
                    val = (val << 4) | (10 + (b - (byte)'a'));
                }
                else if (b >= 'A' && b <= (byte)'F')
                {
                    val = (val << 4) | (10 + (b - (byte)'A'));
                }
                else
                {
                    throw CreateException("Invalid hex value , numeric value is: " + b);
                }
            }
            WriteValToStringBuffer(val);
        }

        private unsafe void WriteValToStringBuffer(int val)
        {
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
            _stringBuffer.Write(smallBuffer, byteCount);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task EnsureBuffer()
        {
            if (_pos >= _bufSize)
                await LoadBufferFromSource();
        }


        public abstract Task LoadBufferFromSource();
        public abstract Task EnsureRestOfToken(byte[] expectedBuffer, string expected);

        protected InvalidDataException CreateException(string message, Exception inner = null)
        {
            var start = Math.Max(0, _pos - 25);
            var count = Math.Min(_pos, _buffer.Length) - start;
            var s = Encoding.UTF8.GetString(_buffer, start, count);
            return new InvalidDataException(message + " at (" + _line + "," + _charPos + ") around: " + s, inner);
        }

        public void Dispose()
        {
            _stringBuffer?.Dispose();
            if (_bufferHandle.IsAllocated)
                _bufferHandle.Free();
        }
    }
}