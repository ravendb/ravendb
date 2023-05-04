using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Pipeline.Parsing
{
    internal static class ScalarTokenizers
    {
        public static int TokenizeWhitespaceAsciiScalar(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            int size = 0;
            int tokenIdx = 0;
            
            int idx = 0;
            for (; idx < buffer.Length; idx++)
            {
                byte b = Unsafe.Add(ref bufferStart, idx);

                if (b > 0x20 || ((ScalarParsers.SingleByteTable >> b) & 1) == 0)
                {
                    size++;
                    continue;
                }

                if (size == 0)
                    continue;

                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = idx - size;
                token.Length = (uint)size;
                token.Type = TokenType.Ascii | TokenType.Word;
                tokenIdx++;

                size = 0;
            }

            if (size != 0)
            {
                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = buffer.Length - size;
                token.Length = (uint)size;
                token.Type = TokenType.Ascii | TokenType.Word;
                tokenIdx++;
            }

            tokens = tokens.Slice(0, tokenIdx);
            return idx;
        }

        public static int TokenizeWhitespace(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            int size = 0;
            int tokenIdx = 0;
            int idx = 0;
            int characterSize;
            for (; idx < buffer.Length; idx += characterSize)
            {
                characterSize = 1;

                byte b = Unsafe.Add(ref bufferStart, idx);
                switch (b)
                {
                    //http://www.unicode.org/versions/Unicode9.0.0/ch03.pdf#page=54 
                    case <= 0b0111_1111:
                        /* 1 byte sequence: 0b0xxxxxxxx */
                        // Nothing to do here.  
                        break;
                    case <= 0b1101_1111:
                        /* 2 byte sequence: 0b110xxxxxx */
                        characterSize = 2;
                        break;
                    case <= 0b1110_1111:
                        /* 0b1110xxxx: 3 bytes sequence */
                        characterSize = 3;
                        break;
                    case <= 0b1111_0111:
                        /* 0b11110xxx: 4 bytes sequence */
                        characterSize = 4;
                        break;
                }
                
                if (b > 0x20 || ((ScalarParsers.SingleByteTable >> b) & 1) == 0)
                {
                    // U+00A0  no-break space
                    if (b == 0xA0)
                        continue;

                    if (idx + 1 < buffer.Length)
                    {
                        size += characterSize;
                        continue;
                    }

                    byte bb = Unsafe.Add(ref bufferStart, idx + 1);
                    switch (b, bb)
                    {
                        case (0x16, 0x80):
                        {
                            // U+1680  ogham space mark
                            break;
                        }
                        case (0x18, 0x0E):
                        {
                            // U+180E  mongolian vowel separator
                            break;
                        }
                        case (0x20, _):
                        {
                            // U+205F  medium mathematical space
                            // U+2060  word joiner
                            if (((ScalarParsers.SecondByte20Table >> bb) & 1) == 0 && bb != 0x5F && bb != 0x60)
                            {
                                size += characterSize;
                                continue;
                            }

                            break;
                        }
                        case (0x30, 0x00):
                        {
                            // U+3000  ideographic space
                            break;
                        }
                        case (0xFE, 0xFF):
                        {
                            // U+FEFF  zero width non-breaking space
                            break;
                        }
                        default:
                        {
                            size += characterSize;
                            continue;
                        }
                    }
                }

                if (size == 0)
                    continue;

                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = idx - size;
                token.Length = (uint)size;
                token.Type = TokenType.Word;
                tokenIdx++;

                size = 0;
            }

            if (size != 0)
            {
                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = buffer.Length - size;
                token.Length = (uint)size;
                token.Type = TokenType.Word;

                tokenIdx++;
            }

            tokens = tokens.Slice(0, tokenIdx);
            return idx;
        }

        public static int TokenizeWhitespace(ReadOnlySpan<char> buffer, ref Span<Token> tokens)
        {
            ref char bufferStart = ref MemoryMarshal.GetReference(buffer);

            int tokenIdx = 0;

            int size = 0;
            int idx = 0;
            for (; idx < buffer.Length; idx++)
            {
                char character = Unsafe.Add(ref bufferStart, idx);

                byte b = (byte)(character >> 8);
                byte bb = (byte)character;

                switch (b, bb)
                {
                    case (0x00, _):
                    {
                        // U+0085  next line
                        // U+00A0  no-break space
                        if (bb > 0x20 || ((ScalarParsers.SingleByteTable >> bb) & 1) == 0)
                        {
                            size++;
                            continue;
                        }
                            
                        break;
                    }
                    case (0x16, 0x80):
                    {
                        // U+1680  ogham space mark
                        break;
                    }
                    case (0x18, 0x0E):
                    {
                        // U+180E  mongolian vowel separator
                        break;
                    }
                    case (0x20, _):
                    {
                        // U+205F  medium mathematical space
                        // U+2060  word joiner
                        if (((ScalarParsers.SecondByte20Table >> bb) & 1) == 0 && bb != 0x5F && bb != 0x60)
                        {
                            size++;
                            continue;
                        }
                        break;
                    }
                    case (0x30, 0x00):
                    {
                        // U+3000  ideographic space
                        break;
                    }
                    case (0xFE, 0xFF):
                    {
                        // U+FEFF  zero width non-breaking space
                        break;
                    }
                    default:
                    {
                        size++;
                        continue;
                    }
                }

                if (size == 0)
                    continue;

                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = idx - size;
                token.Length = (uint)size;
                token.Type = TokenType.Word;

                tokenIdx++;
                size = 0;
            }

            if (size != 0)
            {
                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = idx - size;
                token.Length = (uint)size;
                token.Type = TokenType.Word;

                tokenIdx++;
            }

            tokens = tokens.Slice(0, tokenIdx);
            return (int)idx;
        }
    }
}
