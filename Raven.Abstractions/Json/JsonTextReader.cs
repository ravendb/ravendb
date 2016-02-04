using System;
using System.IO;

namespace Raven.Imports.Newtonsoft.Json
{
    public partial class JsonTextReader
    {
        private byte[] _largeBytesBuffer;
        public Stream ReadBytesAsStream()
        {

            if (TokenType != JsonToken.PropertyName)
                throw new InvalidOperationException("Can only call this on a value");

            char quoteChar;
            // skip whitespace
            while (SkipToNextQouteInsideBuffer(out quoteChar) == false)
            {
                if (ReadData(false) == 0)
                    throw new EndOfStreamException();
            }
            if (_largeBytesBuffer == null)
                _largeBytesBuffer = new byte[1024 * 64];
            var bufferPos = 0;
            FileStream stream = null;
            int buf = 0;
            int iter = 0;
            do
            {
                var bytes = ConvertBase64(_largeBytesBuffer, bufferPos, ref buf, ref iter);
                bufferPos += bytes;
                if (bytes == 0)
                {
                    // flush the remaing values
                    switch (iter)
                    {
                        case 3:
                            _largeBytesBuffer[bufferPos++] = (byte)((buf >> 10) & 255);
                            _largeBytesBuffer[bufferPos++] = (byte)((buf >> 2) & 255);
                            _charPos += 2;
                            break;
                        case 2:
                            _largeBytesBuffer[bufferPos++] = (byte)((buf >> 4) & 255);
                            _charPos++;
                            break;
                    }
                    break;
                }

                if (bufferPos + 4 /*give enough space so we'll be able to flush and account for quote char*/
                    >= _largeBytesBuffer.Length)
                {
                    if (stream == null)
                    {
                        var tempFileName = System.IO.Path.GetTempFileName();

                        stream = new FileStream(tempFileName,
                            FileMode.OpenOrCreate, FileAccess.ReadWrite,
                            FileShare.None, 4096
#if !SILVERLIGHT
                            , FileOptions.SequentialScan | FileOptions.DeleteOnClose
#endif
                            );

                    }
                    stream.Write(_largeBytesBuffer, 0, bufferPos);
                    bufferPos = 0;
                }
                if (_charPos == _charsUsed && ReadData(false) == 0)
                    throw new EndOfStreamException();
            } while (_chars[_charPos] != quoteChar);

            if (_chars[_charPos] == quoteChar)
            {
                _charPos++;
            }
            else
            {
                if (_charPos == _charsUsed)
                {
                    ReadData(false);
                }
                if (_chars[_charPos + 1] == quoteChar)
                    _charPos += 2;
            }

            SetToken(JsonToken.String);
            _currentState = State.PostValue;
            SetPostValueState(true);

            if (stream != null)
            {
                stream.Write(_largeBytesBuffer, 0, bufferPos);
                stream.Position = 0;
                return stream;
            }
            return new MemoryStream(_largeBytesBuffer, 0, bufferPos);
        }


        static readonly byte[] charsLookupTable = {
                66,66,66,66,66,66,66,66,66,66,64,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66,66,66,66,67,66,66,66,66,66,66,66,66,62,66,66,66,63,52,53,
                54,55,56,57,58,59,60,61,66,66,66,65,66,66,66, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,66,66,66,66,66,66,26,27,28,
                29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,66,66,
                66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,66,
                66,66,66,66,66,66
        };

        private const byte WHITESPACE = 64, INVALID = 66, EQUALS = 65, QUOTE = 67;

        private int ConvertBase64(byte[] buffer, int bufferPos, ref int buf, ref int iter)
        {
            if (_charPos == _charsUsed)
                ReadData(false);

            var originBufferPos = bufferPos;
            bool stop = false;
            while (_charPos < _charsUsed &&
                bufferPos < buffer.Length - 2) /*we leave the last two characters free to allow completion*/
            {
                var c = charsLookupTable[_chars[_charPos]];
                switch (c)
                {
                    case WHITESPACE:
                        continue;
                    case INVALID:
                        throw new InvalidOperationException("Found invalid base64 char while reading property into stream -> " + _chars[_charPos] + ", at position " + _charPos);
                    case QUOTE:
                    case EQUALS:
                        stop = true;
                        break;
                    default:
                        buf = buf << 6 | c;
                        iter++; // increment the number of iteration
                                /* If the buffer is full, split it into bytes */
                        if (iter == 4)
                        {
                            buffer[bufferPos++] = (byte)((buf >> 16) & 255);
                            buffer[bufferPos++] = (byte)((buf >> 8) & 255);
                            buffer[bufferPos++] = (byte)(buf & 255);
                            buf = 0;
                            iter = 0;
                        }
                        break;
                }
                if (stop)
                    break;

                _charPos++;
                if (_charPos == _charsUsed)
                    ReadData(false);
            }

            return bufferPos - originBufferPos;
        }

        private bool SkipToNextQouteInsideBuffer(out char quote)
        {
            while (_charPos < _charsUsed)
            {
                var ch = _chars[_charPos++];
                switch (ch)
                {
                    case '"':
                    case '\'':
                        quote = ch;
                        return true;
                }
            }
            quote = '\0';
            return false;
        }

    }
}