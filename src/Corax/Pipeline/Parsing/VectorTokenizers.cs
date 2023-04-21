using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Corax.Pipeline.Parsing
{
    internal static class VectorTokenizers
    {
        public static int TokenizeWhitespaceAscii(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            int N = Vector128<byte>.Count;

            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            nint pos = 0;
            nint len = buffer.Length;

            int size = 0;
            int tokenIdx = 0;

            // process in blocks of 16 bytes when possible
            for (; pos + N < len; pos += N)
            {
                Vector128<byte> bytes = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                Vector128<byte> check = Vector128.Create((byte)0x20);
                Vector128<byte> result = Vector128.LessThanOrEqual(bytes, check);

                uint mask = result.ExtractMostSignificantBits();
                if (mask == 0)
                {
                    // We know that no character is smaller than 0x1F, therefore we continue.
                    size += N;
                    continue;
                }

                // Now we are looking for the ones (the characters that are actually smaller) to test them.
                int lastOffset = 0;
                while (mask != 0)
                {
                    // We count how many non-matches we have in this mask and advance. 
                    int leadingZeroCount = BitOperations.TrailingZeroCount(mask);
                    
                    // This is a match, so we remove it. 
                    mask &= (ushort) ~(1 << leadingZeroCount);

                    // We acquire the actual byte we are gonna process as a match candidate.
                    nint idx = pos + leadingZeroCount;
                    byte b = Unsafe.AddByteOffset(ref bufferStart, idx);
                    if (((ScalarParsers.SingleByteTable >> b) & 1) == 0) 
                        continue; // We know it was a false positive.

                    // We increase the size as this is the current character we are processing. 
                    // This is specially important in the case we have multiple matches in the same vector.
                    size += (leadingZeroCount - lastOffset);

                    // We record this as our new starting point. 
                    lastOffset = leadingZeroCount + 1;

                    if (size == 0)
                        continue;

                    // We found a whitespace.
                    ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                    token.Offset = (int)idx - size;
                    token.Length = (uint)size;
                    token.Type = TokenType.Word;

                    tokenIdx++;
                    size = 0;
                }

                // We increase the size as this is the current character we are processing. 
                // This is specially important in the case we have multiple matches in the same vector.
                size += (N - lastOffset);
            }

            for (; pos < len; pos += 1)
            {
                byte b = Unsafe.AddByteOffset(ref bufferStart, pos);
                if (b > 0x20 || ((ScalarParsers.SingleByteTable >> b) & 1) == 0)
                {
                    size++;
                    continue;
                }

                if (size == 0)
                    continue;

                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = (int)pos - size;
                token.Length = (uint)size;
                token.Type = TokenType.Ascii | TokenType.Word;

                tokenIdx++;
                size = 0;
            }

            if (size != 0)
            {
                ref var token = ref Unsafe.Add(ref MemoryMarshal.GetReference(tokens), tokenIdx);
                token.Offset = (int)pos - size;
                token.Length = (uint)size;
                token.Type = TokenType.Ascii | TokenType.Word;

                tokenIdx++;
            }

            tokens = tokens.Slice(0, tokenIdx);
            return (int)pos;
        }
    }
}
