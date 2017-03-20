using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Sparrow.Json.Parsing
{
    public unsafe class UnmanagedJsonParser : IJsonParser
    {        
        private static readonly byte[] NaN = { (byte)'N', (byte)'a', (byte)'N' };
        public static readonly byte[] Utf8Preamble = Encoding.UTF8.GetPreamble();

        private readonly string _debugTag;
        private UnmanagedWriteBuffer _unmanagedWriteBuffer;
        private string _doubleStringBuffer;
        private int _currentStrStart;
        private readonly JsonOperationContext _ctx;
        private readonly JsonParserState _state;
        private int _pos;
        private int _bufSize;
        private int _line = 1;
        private int _charPos = 1;

        private byte* _inputBuffer;
        private int _prevEscapePosition;
        private byte _currentQuote;

        private byte[] _expectedTokenBuffer;
        private int _expectedTokenBufferPosition;
        private string _expectedTokenString;
        private bool _zeroPrefix;
        private bool _isNegative;
        private bool _isDouble;
        private bool _isExponent;
        private bool _escapeMode;
        private bool _maybeBeforePreamble = true;

        private enum ParseNumberAction
        {
            Fail = 0,
            ParseNumber,
            ParseEnd,
            ParseUnlikely
        }

        private static readonly ParseNumberAction[] ParseNumberTable;        

        static UnmanagedJsonParser()
        {
            ParseStringTable = new byte[255];
            ParseStringTable['r'] = (byte)'\r';
            ParseStringTable['n'] = (byte)'\n';
            ParseStringTable['b'] = (byte)'\b';
            ParseStringTable['f'] = (byte)'\f';
            ParseStringTable['t'] = (byte)'\t';
            ParseStringTable['"'] = (byte)'"';
            ParseStringTable['\\'] = (byte)'\\';
            ParseStringTable['/'] = (byte)'/';
            ParseStringTable['\n'] = Unlikely;
            ParseStringTable['\r'] = Unlikely;
            ParseStringTable['u'] = Unlikely;            

            ParseNumberTable = new ParseNumberAction[255];

            ParseNumberTable['-'] = ParseNumberAction.ParseNumber;
            for (byte ch = (byte)'0'; ch <= '9'; ch++)
                ParseNumberTable[ch] = ParseNumberAction.ParseNumber;

            ParseNumberTable['}'] = ParseNumberTable[']'] = ParseNumberTable[','] = ParseNumberTable[' '] = ParseNumberAction.ParseEnd;
            ParseNumberTable['\t'] = ParseNumberTable['\v'] = ParseNumberTable['\f'] = ParseNumberAction.ParseEnd;

            ParseNumberTable['.'] = ParseNumberTable['e'] = ParseNumberTable['E'] = ParseNumberAction.ParseUnlikely;
            ParseNumberTable['-'] = ParseNumberTable['+'] = ParseNumberAction.ParseUnlikely;
            ParseNumberTable['\r'] = ParseNumberTable['\n'] = ParseNumberAction.ParseUnlikely;
        }

        public UnmanagedJsonParser(JsonOperationContext ctx, JsonParserState state, string debugTag)
        {
            _ctx = ctx;
            _state = state;
            _debugTag = debugTag;
            _unmanagedWriteBuffer = ctx.GetStream();           
        }

        public void SetBuffer(JsonOperationContext.ManagedPinnedBuffer inputBuffer)
        {
            SetBuffer(inputBuffer.Pointer + inputBuffer.Used, inputBuffer.Valid - inputBuffer.Used);
        }

        public void SetBuffer(JsonOperationContext.ManagedPinnedBuffer inputBuffer, int offset, int size)
        {
            SetBuffer(inputBuffer.Pointer + offset, size);
        }
        public void SetBuffer(byte* inputBuffer, int size)
        {
            _inputBuffer = inputBuffer;
            _bufSize = size;
            _pos = 0;
        }

        public int BufferSize
        {
            [MethodImpl((MethodImplOptions.AggressiveInlining))]
            get { return _bufSize; }
        }

        public int BufferOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pos; }
        }


        public void NewDocument()
        {
            _maybeBeforePreamble = true;
            _unmanagedWriteBuffer.Dispose();
            _unmanagedWriteBuffer = _ctx.GetStream();
        }

        public bool Read()
        {
            if (_state.Continuation != JsonParserTokenContinuation.None) // parse normally
            {
                bool read;
                if (ContinueParsingValue(out read))
                    return read;
            }
            
            _state.Continuation = JsonParserTokenContinuation.None;
            if (_maybeBeforePreamble)
            {
                if (ReadMaybeBeforePreamble() == false)
                    return false;
            }

            byte* currentBuffer = _inputBuffer;

            while (true)
            {
                if (_pos >=  _bufSize)
                    return false;

                byte b = currentBuffer[_pos++];
                _charPos++;

                if (b == '\'' || b == '"')
                {
                    _state.EscapePositions.Clear();
                    _unmanagedWriteBuffer.Clear();
                    _prevEscapePosition = 0;
                    _currentQuote = b;
                    _state.CurrentTokenType = JsonParserToken.String;
                    if (ParseString() == false)
                    {
                        _state.Continuation = JsonParserTokenContinuation.PartialString;
                        return false;
                    }
                    _unmanagedWriteBuffer.EnsureSingleChunk(_state);
                    return true;
                }

                switch (b)
                {
                    case (byte)':':
                    case (byte)',':
                        if (_state.CurrentTokenType == JsonParserToken.Separator || _state.CurrentTokenType == JsonParserToken.StartObject || _state.CurrentTokenType == JsonParserToken.StartArray)
                            ThrowException("Cannot have a '" + (char)b + "' in this position");

                        _state.CurrentTokenType = JsonParserToken.Separator;
                        continue;

                    case (byte)'{':
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        return true;
                    case (byte)'[':
                        _state.CurrentTokenType = JsonParserToken.StartArray;
                        return true;
                    case (byte)'}':
                        _state.CurrentTokenType = JsonParserToken.EndObject;
                        return true;
                    case (byte)']':
                        _state.CurrentTokenType = JsonParserToken.EndArray;
                        return true;

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
                    case (byte)'-':
                        {
                            _unmanagedWriteBuffer.Clear();
                            _state.EscapePositions.Clear();
                            _state.Long = 0;
                            _zeroPrefix = b == '0';
                            _isNegative = false;
                            _isDouble = false;
                            _isExponent = false;

                            // ParseNumber need to call _charPos++ & _pos++, so we'll reset them for the first char
                            _pos--;
                            _charPos--;

                            if (ParseNumber() == false)
                            {
                                _state.Continuation = JsonParserTokenContinuation.PartialNumber;
                                return false;
                            }
                            if (_state.CurrentTokenType == JsonParserToken.Float)
                                _unmanagedWriteBuffer.EnsureSingleChunk(_state);
                            return true;
                        }

                    case (byte)'\r':
                        if (_pos >=  _bufSize)
                            return false;
                        if (_inputBuffer[_pos] == (byte)'\n')
                            continue;
                        goto case (byte)'\n';

                    case (byte)'\n':
                        _line++;
                        _charPos = 1;
                        continue;

                    case (byte)' ':
                    case (byte)'\t':
                    case (byte)'\v':
                    case (byte)'\f':
                        //white space, we can safely ignore
                        continue; 

                    case (byte)'N':
                        _unmanagedWriteBuffer.Clear();
                        _state.CurrentTokenType = JsonParserToken.Float;
                        _expectedTokenBuffer = NaN;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "NaN";
                        if (EnsureRestOfToken() == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialNaN;
                            return false;
                        }
                        _unmanagedWriteBuffer.Write(NaN,0, NaN.Length);
                        _unmanagedWriteBuffer.EnsureSingleChunk(_state);
                        return true;
                    case (byte)'n':
                        _state.CurrentTokenType = JsonParserToken.Null;
                        _expectedTokenBuffer = BlittableJsonTextWriter.NullBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "null";
                        if (EnsureRestOfToken() == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialNull;
                            return false;
                        }
                        return true;
                    case (byte)'t':
                        _state.CurrentTokenType = JsonParserToken.True;
                        _expectedTokenBuffer = BlittableJsonTextWriter.TrueBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "true";
                        if (EnsureRestOfToken() == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialTrue;
                            return false;
                        }
                        return true;
                    case (byte)'f':
                        _state.CurrentTokenType = JsonParserToken.False;
                        _expectedTokenBuffer = BlittableJsonTextWriter.FalseBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "false";
                        if (EnsureRestOfToken() == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialFalse;
                            return false;
                        }
                        return true;
                }

                ThrowCannotHaveCharInThisPosition(b);
            }
        }

        private void ThrowCannotHaveCharInThisPosition(byte b)
        {
            ThrowException("Cannot have a '" + (char) b + "' in this position");
        }

        private bool ReadMaybeBeforePreamble()
        {
            if (_pos >= _bufSize)
            {
                return false;
            }

            if (_inputBuffer[_pos] == Utf8Preamble[0])
            {
                _pos++;
                _expectedTokenBuffer = Utf8Preamble;
                _expectedTokenBufferPosition = 1;
                _expectedTokenString = "UTF8 Preamble";
                if (EnsureRestOfToken() == false)
                {
                    _state.Continuation = JsonParserTokenContinuation.PartialPreamble;
                    return false;
                }
            }
            else
            {
                _maybeBeforePreamble = false;
            }
            return true;
        }

        private bool ContinueParsingValue(out bool read)
        {
            read = false;
            switch (_state.Continuation)
            {
                case JsonParserTokenContinuation.PartialNaN:
                {
                    if (EnsureRestOfToken() == false)
                        return true;

                    _state.Continuation = JsonParserTokenContinuation.None;
                    _state.CurrentTokenType = JsonParserToken.Float;
                    _unmanagedWriteBuffer.EnsureSingleChunk(_state);

                    read = true;
                    return true;
                }
                case JsonParserTokenContinuation.PartialNumber:
                {
                    if (ParseNumber() == false)
                        return true;

                    if (_state.CurrentTokenType == JsonParserToken.Float)
                        _unmanagedWriteBuffer.EnsureSingleChunk(_state);

                    _state.Continuation = JsonParserTokenContinuation.None;

                    read = true;
                    return true;

                }
                case JsonParserTokenContinuation.PartialPreamble:
                {
                    if (EnsureRestOfToken() == false)
                        return true;

                    _state.Continuation = JsonParserTokenContinuation.None;

                    break; // single case where we don't return 
                }
                case JsonParserTokenContinuation.PartialString:
                {
                    if (ParseString() == false)
                        return true;

                    _unmanagedWriteBuffer.EnsureSingleChunk(_state);
                    _state.CurrentTokenType = JsonParserToken.String;
                    _state.Continuation = JsonParserTokenContinuation.None;
  
                    read = true;
                    return true;

                }
                case JsonParserTokenContinuation.PartialFalse:
                {
                    if (EnsureRestOfToken() == false)
                        return true;

                    _state.CurrentTokenType = JsonParserToken.False;
                    _state.Continuation = JsonParserTokenContinuation.None;

                    read = true;
                    return true;

                }
                case JsonParserTokenContinuation.PartialTrue:
                {
                    if (EnsureRestOfToken() == false)
                        return true;

                    _state.CurrentTokenType = JsonParserToken.True;
                    _state.Continuation = JsonParserTokenContinuation.None;

                    read = true;
                    return true;
                }
                case JsonParserTokenContinuation.PartialNull:
                {
                    if (EnsureRestOfToken() == false)
                        return true;

                    _state.CurrentTokenType = JsonParserToken.Null;
                    _state.Continuation = JsonParserTokenContinuation.None;

                    read = true;
                    return true;
                }
                default:
                    ThrowException("Somehow got continuation for single byte token " + _state.Continuation);
                    return false; // never hit
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ParseNumber()
        {
            JsonParserState state = _state;
            byte* currentBuffer = _inputBuffer;

            while (true)
            {
                if (_pos >= _bufSize)
                    return false;
                _charPos++;

                byte b = currentBuffer[_pos++];
                if (b >= '0' && b <= '9')
                {
                    // PERF: This is a fast loop for the most common characters found on numbers.
                    state.Long = (state.Long * 10) + b - (byte)'0';
                    _unmanagedWriteBuffer.WriteByte(b);

                    continue;
                }

                if (b == ' ' || b == ',' || b == '}' || b == ']' || ParseNumberTable[b] == ParseNumberAction.ParseEnd)
                {
                    if (!_zeroPrefix || _unmanagedWriteBuffer.SizeInBytes == 1)
                    {
                        if (_isNegative)
                            state.Long *= -1;

                        state.CurrentTokenType = _isDouble ? JsonParserToken.Float : JsonParserToken.Integer;

                        _pos--; _charPos--;// need to re-read this char

                        return true;
                    }

                    ThrowWhenMalformed("Invalid number with zero prefix");
                    break;
                }
            
                if (ParseNumberTable[b] == ParseNumberAction.ParseUnlikely)
                {
                    if (ParseNumberUnlikely(b, state))
                        return true;

                    _unmanagedWriteBuffer.WriteByte(b);
                    continue;
                }

                // No hit, we are done.
                ThrowWhenMalformed("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
            }

            return false; // Will never execute.
        }

        private bool ParseNumberUnlikely(byte b, JsonParserState state)
        {
            switch (b)
            {
                case (byte)'.':
                {
                    if (!_isDouble)
                    {
                        _zeroPrefix = false; // 0.5, frex
                        _isDouble = true;
                        break;
                    }

                    ThrowWhenMalformed("Already got '.' in this number value");
                    break;
                }
                case (byte)'+':
                    break; // just record, appears in 1.4e+3
                case (byte)'e':
                case (byte)'E':
                {
                    if (_isExponent)
                        ThrowWhenMalformed("Already got 'e' in this number value");
                    _isExponent = true;
                    _isDouble = true;
                    break;
                }
                case (byte)'-':
                {
                    if (!_isNegative || _isExponent != false)
                    {
                        _isNegative = true;
                        break;
                    }

                    ThrowWhenMalformed("Already got '-' in this number value");
                    break;
                }

                case (byte)'\r':
                case (byte)'\n':
                { 
                    _line++;
                    _charPos = 1;

                    if (!_zeroPrefix || _unmanagedWriteBuffer.SizeInBytes == 1)
                    {
                        if (_isNegative)
                            state.Long *= -1;

                        state.CurrentTokenType = _isDouble ? JsonParserToken.Float : JsonParserToken.Integer;

                        _pos--;
                        _charPos--; // need to re-read this char

                        return true;
                    }

                    ThrowWhenMalformed("Invalid number with zero prefix");
                    break;
                }
            }

            return false;
        }

        private void ThrowWhenMalformed(string message)
        {
            ThrowException(message);
        }

        public bool EnsureRestOfToken()
        {
            for (int i = _expectedTokenBufferPosition; i < _expectedTokenBuffer.Length; i++)
            {
                if (_pos >= _bufSize)
                    return false;
                if (_inputBuffer[_pos++] != _expectedTokenBuffer[i])
                    ThrowException("Invalid token found, expected: " + _expectedTokenString);
                _expectedTokenBufferPosition++;
                _charPos++;
            }
            return true;
        }

        private const byte NoSubstitution = 0;
        private const byte Unlikely = 1;
        private static readonly byte[] ParseStringTable;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ParseString()
        {
            byte* currentBuffer = _inputBuffer;
            byte[] parseStringTable = ParseStringTable;

            while (true)
            {
                _currentStrStart = _pos;

                while (_pos < _bufSize)
                {
                    byte b = currentBuffer[_pos++];
                    _charPos++;

                    if (_escapeMode == false)
                    {
                        // PERF: Early escape to avoid jumping around in the code layout.
                        if (b != _currentQuote && b != (byte) '\\')
                            continue;

                        _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, _pos - _currentStrStart - 1 /* don't include the escape or the last quote */);

                        if (b == _currentQuote)
                            return true;

                        // Then it is '\\'
                        _escapeMode = true;
                        _currentStrStart = _pos;                        
                    }
                    else
                    {
                        _currentStrStart++;
                        _escapeMode = false;
                        _charPos++;
                        if (b != (byte) 'u' && b != (byte) '/')
                        {
                            _state.EscapePositions.Add(_unmanagedWriteBuffer.SizeInBytes - _prevEscapePosition);
                            _prevEscapePosition = _unmanagedWriteBuffer.SizeInBytes + 1;
                        }

                        byte op = parseStringTable[b];
                        if (op > Unlikely)
                        {
                            // We have a known substitution to apply
                            _unmanagedWriteBuffer.WriteByte(op);
                        }
                        else if (b == (byte)'\n')
                        {
                            _line++;
                            _charPos = 1;
                        }
                        else if (b == (byte)'\r')
                        {
                            if (_pos >= _bufSize)
                                return false;

                            _line++;
                            _charPos = 1;
                            if (_pos >= _bufSize)
                                return false;

                            if (currentBuffer[_pos] == (byte)'\n')
                                _pos++; // consume the \,\r,\n
                        }
                        else if (b == (byte)'u')
                        {
                            if (ParseUnicodeValue() == false)
                                return false;
                        }
                        else
                        {
                            ThrowInvalidEscapeChar(b);
                        }
                    }
                }

                // copy the buffer to the native code, then refill
                _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, _pos - _currentStrStart);

                if (_pos >= _bufSize)
                    return false;
            }
        }

        private static void ThrowInvalidEscapeChar(byte b)
        {
            throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
        }


        private bool ParseUnicodeValue()
        {
            byte b;
            int val = 0;
            for (int i = 0; i < 4; i++)
            {
                if (_pos >= _bufSize)
                    return false;

                b = _inputBuffer[_pos++];
                _currentStrStart++;
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
                    ThrowException("Invalid hex value , numeric value is: " + b);
                }
            }
            WriteUnicodeCharacterToStringBuffer(val);
            return true;
        }

        private void WriteUnicodeCharacterToStringBuffer(int val)
        {
            var smallBuffer = stackalloc byte[8];
            var chars = stackalloc char[1];
            try
            {
                chars[0] = Convert.ToChar(val);
            }
            catch (Exception e)
            {
                throw new FormatException("Could not convert value " + val + " to char", e);
            }
            var byteCount = Encoding.UTF8.GetBytes(chars, 1, smallBuffer, 8);
            _unmanagedWriteBuffer.Write(smallBuffer, byteCount);
        }


        public void ValidateFloat()
        {
            if (_unmanagedWriteBuffer.SizeInBytes > 100)
                ThrowException("Too many characters in double: " + _unmanagedWriteBuffer.SizeInBytes);

            if (_doubleStringBuffer == null || _unmanagedWriteBuffer.SizeInBytes > _doubleStringBuffer.Length)
                _doubleStringBuffer = new string(' ', _unmanagedWriteBuffer.SizeInBytes);
          
            var tmpBuff = stackalloc byte[_unmanagedWriteBuffer.SizeInBytes];
            // here we assume a clear char <- -> byte conversion, we only support
            // utf8, and those cleanly transfer
            fixed (char* pChars = _doubleStringBuffer)
            {
                int i = 0;
                _unmanagedWriteBuffer.CopyTo(tmpBuff);
                for (; i < _unmanagedWriteBuffer.SizeInBytes; i++)
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
                double.Parse(_doubleStringBuffer, NumberStyles.Any, CultureInfo.InvariantCulture);
            }
            catch (Exception e)
            {
                ThrowException("Could not parse double", e);
            }
        }


        protected void ThrowException(string message, Exception inner = null)
        {
            throw new InvalidDataException($"{message} at {GenerateErrorState()}", inner);
        }

        public void Dispose()
        {
            _unmanagedWriteBuffer.Dispose();
        }

        public string GenerateErrorState()
        {
            var s = Encoding.UTF8.GetString(_inputBuffer, _bufSize);
            return " (" + _line + "," + _charPos + ") around: " + s;
        }
    }
}