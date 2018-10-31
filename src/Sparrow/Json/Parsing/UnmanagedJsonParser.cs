using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Sparrow.Json.Parsing
{
    public unsafe class UnmanagedJsonParser : IJsonParser
    {
        private static readonly byte[] NaN = { (byte)'N', (byte)'a', (byte)'N' };
        private static readonly byte[] PositiveInfinity =
        {
            (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y'
        };
        private static readonly byte[] NegativeInfinity =
        {
            (byte)'-', (byte)'I', (byte)'n', (byte)'f', (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'y'
        };

        public static readonly byte[] Utf8Preamble = Encoding.UTF8.GetPreamble();

        private readonly string _debugTag;
        private UnmanagedWriteBuffer _unmanagedWriteBuffer;        
        private int _currentStrStart;
        private readonly JsonOperationContext _ctx;
        private readonly JsonParserState _state;
        private uint _pos;
        private uint _bufSize;
        private int _line = 1;
        private uint _charPos = 1;

        private byte* _inputBuffer;
        private int _prevEscapePosition;
        private byte _currentQuote;

        private byte[] _expectedTokenBuffer;
        private int _expectedTokenBufferPosition;
        private string _expectedTokenString;
        private bool _zeroPrefix;
        private bool _isNegative;
        private bool _isFractionedDouble;
        private bool _isOverflow;

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
            _unmanagedWriteBuffer = ctx.GetStream(JsonOperationContext.InitialStreamSize);
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
            _bufSize = (uint)size;
            _pos = 0;
        }

        public int BufferSize
        {
            [MethodImpl((MethodImplOptions.AggressiveInlining))]
            get { return (int)_bufSize; }
        }

        public int BufferOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (int)_pos; }
        }


        public void NewDocument()
        {
            _maybeBeforePreamble = true;
            var previous = _unmanagedWriteBuffer.SizeInBytes;
            _unmanagedWriteBuffer.Dispose();
            _unmanagedWriteBuffer = _ctx.GetStream(previous);
        }

        public (bool Done, int BytesRead) Copy(byte* output, int count)
        {
            var amountToCopy = Math.Min(count, _bufSize - _pos);
            Memory.Copy(output, _inputBuffer + _pos, amountToCopy);
            _pos += (uint)amountToCopy;
            return (count == amountToCopy, (int)amountToCopy);
        }

        public (bool Done, int BytesRead) Skip(int count)
        {
            var amountToCopy = Math.Min(count, _bufSize - _pos);
            _pos += (uint)amountToCopy;
            return (count == amountToCopy, (int)amountToCopy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read()
        {
            var state = _state;
            if (state.Continuation != JsonParserTokenContinuation.None || _maybeBeforePreamble)
                goto ReadContinuation;

MainLoop:

            byte b;
            byte* currentBuffer = _inputBuffer;
            uint bufferSize = _bufSize;
            uint pos = _pos;
            while (true)
            {
                if (pos >= bufferSize)
                    goto ReturnFalse;

                b = currentBuffer[pos];
                pos++;
                _charPos++;

                if (b == ':' || b == ',')
                {
                    if (state.CurrentTokenType == JsonParserToken.Separator || state.CurrentTokenType == JsonParserToken.StartObject || state.CurrentTokenType == JsonParserToken.StartArray)
                        goto Error;

                    state.CurrentTokenType = JsonParserToken.Separator;
                    continue;
                }

                if (b == '\'' || b == '"')
                    goto ParseString; // PERF: Avoid very lengthy method here; as we are going to return anyways.

                if ((b >= '0' && b <= '9') || IsPossibleNegativeNumber(b, bufferSize, pos, currentBuffer))
                    goto ParseNumber; // PERF: Avoid very lengthy method here; as we are going to return anyways.

                if (b == '{')
                {
                    state.CurrentTokenType = JsonParserToken.StartObject;
                    goto ReturnTrue;
                }

                if (b == '}')
                {
                    state.CurrentTokenType = JsonParserToken.EndObject;
                    goto ReturnTrue;
                }
                if (b == '[')
                {
                    state.CurrentTokenType = JsonParserToken.StartArray;
                    goto ReturnTrue;
                }
                if (b == ']')
                {
                    state.CurrentTokenType = JsonParserToken.EndArray;
                    goto ReturnTrue;
                }

                bool couldRead;
                if (!ReadUnlikely(b, ref pos, out couldRead))
                    continue; // We can only continue here, if there is a failure to parse, we will throw inside ReadUnlikely.

                if (couldRead)
                    goto ReturnTrue;
                goto ReturnFalse;
            }

ParseString:
            {
                state.EscapePositions.Clear();
                _unmanagedWriteBuffer.Clear();
                _prevEscapePosition = 0;
                _currentQuote = b;
                state.CurrentTokenType = JsonParserToken.String;
                if (ParseString(ref pos) == false)
                {
                    state.Continuation = JsonParserTokenContinuation.PartialString;
                    goto ReturnFalse;
                }
                _unmanagedWriteBuffer.EnsureSingleChunk(state);
                goto ReturnTrue;
            }

ParseNumber:
            {
                _unmanagedWriteBuffer.Clear();
                state.EscapePositions.Clear();
                state.Long = 0;
                _zeroPrefix = b == '0';
                _isNegative = false;
                _isFractionedDouble = false;
                _isExponent = false;
                _isOverflow = false;

                // ParseNumber need to call _charPos++ & _pos++, so we'll reset them for the first char
                pos--;
                _charPos--;

                if (ParseNumber(ref state.Long, ref pos) == false)
                {
                    state.Continuation = JsonParserTokenContinuation.PartialNumber;
                    goto ReturnFalse;
                }

                if (state.CurrentTokenType == JsonParserToken.Float)
                    _unmanagedWriteBuffer.EnsureSingleChunk(state);
                goto ReturnTrue;
            }

Error:
            ThrowCannotHaveCharInThisPosition(b);

ReturnTrue:
            _pos = pos;
            return true;

ReturnFalse:
            _pos = pos;
            return false;


ReadContinuation: // PERF: This is a "manual procedure"
            if (state.Continuation != JsonParserTokenContinuation.None) // parse normally
            {
                return ContinueParsingValue();
            }

            state.Continuation = JsonParserTokenContinuation.None;
            if (_maybeBeforePreamble)
            {
                if (ReadMaybeBeforePreamble() == false)
                    return false;
            }

            goto MainLoop;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsPossibleNegativeNumber(byte b, uint bufferSize, uint pos, byte* currentBuffer)
        {
            if (b != '-')
                return false;

            if (pos >= bufferSize)
                return false;

            var nextChar = currentBuffer[pos];
            // -Infinity is saved as a string
            return nextChar != 'I';
        }

        private bool ReadUnlikely(byte b, ref uint pos, out bool couldRead)
        {
            couldRead = false;
            switch (b)
            {
                case (byte)'\r':
                    {
                        if (pos >= _bufSize)
                        {
                            return true;
                        }
                        if (_inputBuffer[pos] == (byte)'\n')
                        {
                            return false;
                        }
                        goto case (byte)'\n';
                    }

                case (byte)'\n':
                    {
                        _line++;
                        _charPos = 1;
                        return false;
                    }

                case (byte)' ':
                case (byte)'\t':
                case (byte)'\v':
                case (byte)'\f':
                    //white space, we can safely ignore
                    return false;

                case (byte)'N':
                    {
                        ReadToken(ref pos, ref couldRead, NaN, "NaN", JsonParserTokenContinuation.PartialNaN);
                        return true;
                    }

                case (byte)'I':
                    {
                        ReadToken(ref pos, ref couldRead, PositiveInfinity, "Infinity", JsonParserTokenContinuation.PartialPositiveInfinity);
                        return true;
                    }

                case (byte)'-':
                    {
                        _zeroPrefix = false;
                        ReadToken(ref pos, ref couldRead, NegativeInfinity, "-Infinity", JsonParserTokenContinuation.PartialNegativeInfinity);
                        return true;
                    }

                case (byte)'n':
                    {
                        _state.CurrentTokenType = JsonParserToken.Null;
                        _expectedTokenBuffer = BlittableJsonTextWriter.NullBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "null";
                        if (EnsureRestOfToken(ref pos) == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialNull;
                            return true;
                        }

                        couldRead = true;
                        return true;
                    }

                case (byte)'t':
                    {
                        _state.CurrentTokenType = JsonParserToken.True;
                        _expectedTokenBuffer = BlittableJsonTextWriter.TrueBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "true";
                        if (EnsureRestOfToken(ref pos) == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialTrue;
                            return true;
                        }

                        couldRead = true;
                        return true;
                    }

                case (byte)'f':
                    {
                        _state.CurrentTokenType = JsonParserToken.False;
                        _expectedTokenBuffer = BlittableJsonTextWriter.FalseBuffer;
                        _expectedTokenBufferPosition = 1;
                        _expectedTokenString = "false";
                        if (EnsureRestOfToken(ref pos) == false)
                        {
                            _state.Continuation = JsonParserTokenContinuation.PartialFalse;
                            return true;
                        }

                        couldRead = true;
                        return true;
                    }
            }

            ThrowCannotHaveCharInThisPosition(b);
            return false;
        }

        private void ReadToken(ref uint pos, ref bool couldRead, byte[] tokenBuffer,
            string tokenString, JsonParserTokenContinuation jsonParserTokenContinuation)
        {
            _unmanagedWriteBuffer.Clear();
            _state.CurrentTokenType = JsonParserToken.Float;
            _expectedTokenBuffer = tokenBuffer;
            _expectedTokenBufferPosition = 1;
            _expectedTokenString = tokenString;
            if (EnsureRestOfToken(ref pos) == false)
            {
                _state.Continuation = jsonParserTokenContinuation;
                return;
            }

            _unmanagedWriteBuffer.Write(tokenBuffer, 0, tokenBuffer.Length);
            _unmanagedWriteBuffer.EnsureSingleChunk(_state);
            couldRead = true;
        }

        private void ThrowCannotHaveCharInThisPosition(byte b)
        {
            ThrowException("Cannot have a '" + (char)b + "' in this position");
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
                if (EnsureRestOfToken(ref _pos) == false)
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

        private bool ContinueParsingValue()
        {
            switch (_state.Continuation)
            {
                case JsonParserTokenContinuation.PartialNegativeInfinity:
                    // here we need to check if we have a negative number or negative
                    // infinity 
                    if (_expectedTokenBufferPosition == 1 &&
                        _inputBuffer[_pos] != (byte)'I')
                    {
                        _zeroPrefix = false;
                        _isNegative = true;
                        _isFractionedDouble = false;
                        _isOverflow = false;
                        _isExponent = false;
                        _state.Long = 0;
                        _state.EscapePositions.Clear();
                        _unmanagedWriteBuffer.Clear();
                        _unmanagedWriteBuffer.WriteByte((byte)'-');
                        _state.Continuation = JsonParserTokenContinuation.PartialNumber;
                        goto case JsonParserTokenContinuation.PartialNumber;
                    }
                    goto case JsonParserTokenContinuation.PartialPositiveInfinity;
                case JsonParserTokenContinuation.PartialNaN:
                case JsonParserTokenContinuation.PartialPositiveInfinity:
                    {
                        if (EnsureRestOfToken(ref _pos) == false)
                            return false;

                        _state.Continuation = JsonParserTokenContinuation.None;
                        _state.CurrentTokenType = JsonParserToken.Float;
                        _unmanagedWriteBuffer.Write(_expectedTokenBuffer, 0, _expectedTokenBuffer.Length);
                        _unmanagedWriteBuffer.EnsureSingleChunk(_state);

                        return true;
                    }
                case JsonParserTokenContinuation.PartialNumber:
                    {
                        if (ParseNumber(ref _state.Long, ref _pos) == false)
                            return false;

                        if (_state.CurrentTokenType == JsonParserToken.Float)
                            _unmanagedWriteBuffer.EnsureSingleChunk(_state);

                        _state.Continuation = JsonParserTokenContinuation.None;

                        return true;

                    }
                case JsonParserTokenContinuation.PartialPreamble:
                    {
                        if (EnsureRestOfToken(ref _pos) == false)
                            return false;

                        _state.Continuation = JsonParserTokenContinuation.None;

                        return false;
                    }
                case JsonParserTokenContinuation.PartialString:
                    {
                        if (ParseString(ref _pos) == false)
                            return false;

                        _unmanagedWriteBuffer.EnsureSingleChunk(_state);
                        _state.CurrentTokenType = JsonParserToken.String;
                        _state.Continuation = JsonParserTokenContinuation.None;

                        return true;

                    }
                case JsonParserTokenContinuation.PartialFalse:
                    {
                        if (EnsureRestOfToken(ref _pos) == false)
                            return false;

                        _state.CurrentTokenType = JsonParserToken.False;
                        _state.Continuation = JsonParserTokenContinuation.None;

                        return true;

                    }
                case JsonParserTokenContinuation.PartialTrue:
                    {
                        if (EnsureRestOfToken(ref _pos) == false)
                            return false;

                        _state.CurrentTokenType = JsonParserToken.True;
                        _state.Continuation = JsonParserTokenContinuation.None;

                        return true;
                    }
                case JsonParserTokenContinuation.PartialNull:
                    {
                        if (EnsureRestOfToken(ref _pos) == false)
                            return false;

                        _state.CurrentTokenType = JsonParserToken.Null;
                        _state.Continuation = JsonParserTokenContinuation.None;

                        return true;
                    }
                default:
                    ThrowException("Somehow got continuation for single byte token " + _state.Continuation);
                    return false; // never hit
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ParseNumber(ref long value, ref uint pos)
        {
            JsonParserState state = _state;

            uint bufferSize = _bufSize;
            byte* inputBuffer = _inputBuffer;
            while (true)
            {
                if (pos >= bufferSize)
                    goto NotANumber;

                byte b = inputBuffer[pos];
                byte digit = (byte)(b - (byte)'0');

                _charPos++;
                pos++;
                if (digit <= 9) // PERF: Simplified the check to get rid of 1 comparison
                {
                    // PERF: This is a fast loop for the most common characters found on numbers.
                    var next = (value * 10) + digit;

                    if (next < value) //overflow
                        _isOverflow = true;

                    value = next;


                    _unmanagedWriteBuffer.WriteByte(b);

                    continue;
                }

                if (b == ' ' || b == ',' || b == '}' || b == ']' || ParseNumberTable[b] == ParseNumberAction.ParseEnd)
                {
                    if (!_zeroPrefix || _unmanagedWriteBuffer.SizeInBytes == 1)
                    {
                        if (_isNegative)
                            value *= -1;

                        state.CurrentTokenType = (_isFractionedDouble || _isOverflow) ? JsonParserToken.Float : JsonParserToken.Integer;

                        pos--;
                        _charPos--;// need to re-read this char

                        goto IsANumber;
                    }

                    ThrowWhenMalformed("Invalid number with zero prefix");
                    break;
                }

                if (ParseNumberTable[b] == ParseNumberAction.ParseUnlikely)
                {
                    if (ParseNumberUnlikely(b, ref pos, ref value, state))
                        goto IsANumber;

                    _unmanagedWriteBuffer.WriteByte(b);
                    continue;
                }

                // No hit, we are done.
                ThrowWhenMalformed("Number cannot end with char with: '" + (char)b + "' (" + b + ")");
            }

IsANumber:
            return true;

NotANumber:
            return false; // Will never execute.
        }

        private bool ParseNumberUnlikely(byte b, ref uint pos, ref long value, JsonParserState state)
        {
            switch (b)
            {
                case (byte)'.':
                    {
                        if (!_isFractionedDouble)
                        {
                            _zeroPrefix = false; // 0.5, frex
                            _isFractionedDouble = true;
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
                        _isFractionedDouble = true;
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
                                value *= -1;

                            state.CurrentTokenType = (_isFractionedDouble || _isOverflow) ? JsonParserToken.Float : JsonParserToken.Integer;

                            pos--;
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

        private bool EnsureRestOfToken(ref uint pos)
        {
            uint bufferSize = _bufSize;
            byte* inputBuffer = _inputBuffer;
            byte[] expectedTokenBuffer = _expectedTokenBuffer;
            for (int i = _expectedTokenBufferPosition; i < expectedTokenBuffer.Length; i++)
            {
                if (pos >= bufferSize)
                    return false;

                if (inputBuffer[pos++] != expectedTokenBuffer[i])
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
        private bool ParseString(ref uint currentPos)
        {
            byte* currentBuffer = _inputBuffer;
            byte[] parseStringTable = ParseStringTable;

            uint bufferSize = _bufSize;

            while (true)
            {
                _currentStrStart = (int)currentPos;

                while (currentPos < bufferSize)
                {
                    byte b = currentBuffer[currentPos];
                    currentPos++;
                    _charPos++;

                    if (_escapeMode == false)
                    {
                        // PERF: Early escape to avoid jumping around in the code layout.
                        if (b != _currentQuote && b != (byte)'\\')
                            continue;

                        _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, (int)currentPos - _currentStrStart - 1 /* don't include the escape or the last quote */);

                        if (b == _currentQuote)
                            goto ReturnTrue;

                        // Then it is '\\'
                        _escapeMode = true;
                        _currentStrStart = (int)currentPos;
                    }
                    else
                    {
                        _currentStrStart++;
                        _escapeMode = false;
                        _charPos++;
                        if (b != (byte)'u' && b != (byte)'/')
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
                            if (currentPos >= bufferSize)
                                goto ReturnFalse;

                            _line++;
                            _charPos = 1;
                            if (currentPos >= bufferSize)
                                goto ReturnFalse;

                            if (currentBuffer[currentPos] == (byte)'\n')
                                currentPos++; // consume the \,\r,\n
                        }
                        else if (b == (byte)'u')
                        {
                            if (ParseUnicodeValue(ref currentPos) == false)
                                goto ReturnFalse;
                        }
                        else
                        {
                            ThrowInvalidEscapeChar(b);
                        }
                    }
                }

                // copy the buffer to the native code, then refill
                _unmanagedWriteBuffer.Write(currentBuffer + _currentStrStart, (int)currentPos - _currentStrStart);

                if (currentPos >= bufferSize)
                    goto ReturnFalse;
            }


ReturnTrue:
            return true;

ReturnFalse:
            return false;
        }

        private static void ThrowInvalidEscapeChar(byte b)
        {
            throw new InvalidOperationException("Invalid escape char, numeric value is " + b);
        }


        private bool ParseUnicodeValue(ref uint pos)
        {
            byte b;
            int val = 0;

            byte* inputBuffer = _inputBuffer;
            uint bufferSize = _bufSize;
            for (int i = 0; i < 4; i++)
            {
                if (pos >= bufferSize)
                    return false;

                b = inputBuffer[pos];
                pos++;
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
            if(val < 32)
            {
                // control character
                var esc = _unmanagedWriteBuffer.SizeInBytes - _prevEscapePosition;
                _state.EscapePositions.Add(esc);
                _prevEscapePosition = _unmanagedWriteBuffer.SizeInBytes + 1;
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
            var byteCount = Encodings.Utf8.GetBytes(chars, 1, smallBuffer, 8);
            _unmanagedWriteBuffer.Write(smallBuffer, byteCount);
        }


        public void ValidateFloat()
        {
            try
            {
                int numLength = _unmanagedWriteBuffer.SizeInBytes;

                if (numLength <= 100)
                {
                    byte* tmpBuff = stackalloc byte[numLength];
                    _unmanagedWriteBuffer.CopyTo(tmpBuff);
                    _ctx.ParseDouble(tmpBuff, numLength);
                }
                else
                {
                    var memoryForNumber = _ctx.GetMemory(numLength);

                    try
                    {
                        _unmanagedWriteBuffer.CopyTo(memoryForNumber.Address);                        
                        _ctx.ParseDouble(memoryForNumber.Address, numLength);
                    }
                    finally
                    {
                        _ctx.ReturnMemory(memoryForNumber);
                    }

                }
                
            }
#pragma warning disable RDB0004 // Exception handler is empty or just logging
            catch (Exception e)
            {
                ThrowException("Could not parse double", e);                
            }
#pragma warning restore RDB0004 // Exception handler is empty or just logging
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
            var s = Encodings.Utf8.GetString(_inputBuffer, (int)_bufSize);
            return " (" + _line + "," + _charPos + ") around: " + s;
        }
    }
}
