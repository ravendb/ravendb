using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Corax.Pipeline.Parsing
{
    internal static class ScalarTransformers
    {
        // Lucene's LowerCaseKeywordTokenizer lowercases per UTF-16 code unit. Surrogate code units have no case
        // mapping, so characters outside the BMP (i.e. encoded as surrogate pairs) are left unchanged by Lucene.
        // To keep Corax terms byte-for-byte identical to Lucene, we must NOT case fold non-BMP runes here.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Rune ToLowerInvariantLikeLucene(Rune rune) => rune.IsBmp ? Rune.ToLowerInvariant(rune) : rune;

        public static int ToLowercaseAscii(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            nint pos = 0;
            nint len = source.Length;

            ref byte sourceStart = ref MemoryMarshal.GetReference(source);
            ref byte destStart = ref MemoryMarshal.GetReference(dest);
            ref byte lowerCaseTable = ref MemoryMarshal.GetReference(StandardTransformers.LowercaseTable);

            const int N32 = sizeof(uint); // process in blocks of 4 bytes when possible

            for (; pos + N32 <= len; pos += N32)
            {
                // We will see if we can discard 4 bytes straight from a single read. 
                var value = Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref sourceStart, pos));

                // We are testing !((ch & CASE_MASK) ^ ('A' & CASE_MASK)) 
                if (((value & ParsingConstants.UppercaseUInt32Mask) ^ (0x41414141 & ParsingConstants.UppercaseUInt32Mask)) == 0x20202020)
                {
                    // We know that no uppercase has been found in the 4 bytes block. So this becomes a very fast pass-through.
                    Unsafe.WriteUnaligned<uint>(ref Unsafe.AddByteOffset(ref destStart, pos), value);
                    continue;
                }

                // dest[pos] = source[pos] | lowercaseTable[source[pos]]
                ref byte d1 = ref Unsafe.AddByteOffset(ref destStart, pos);
                byte b1 = Unsafe.AddByteOffset(ref sourceStart, pos);
                d1 = (byte)(b1 | Unsafe.AddByteOffset(ref lowerCaseTable, b1));

                // dest[pos+1] = source[pos+1] | lowercaseTable[source[pos+1]]
                ref byte d2 = ref Unsafe.AddByteOffset(ref destStart, pos + 1);
                byte b2 = Unsafe.AddByteOffset(ref sourceStart, pos + 1);
                d2 = (byte)(b2 | Unsafe.AddByteOffset(ref lowerCaseTable, b2));

                // dest[pos+2] = source[pos+2] | lowercaseTable[source[pos+2]]
                ref byte d3 = ref Unsafe.AddByteOffset(ref destStart, pos + 2);
                byte b3 = Unsafe.AddByteOffset(ref sourceStart, pos + 2);
                d3 = (byte)(b3 | Unsafe.AddByteOffset(ref lowerCaseTable, b3));

                // dest[pos+3] = source[pos+3] | lowercaseTable[source[pos+3]]
                ref byte d4 = ref Unsafe.AddByteOffset(ref destStart, pos + 3);
                byte b4 = Unsafe.AddByteOffset(ref sourceStart, pos + 3);
                d4 = (byte)(b4 | Unsafe.AddByteOffset(ref lowerCaseTable, b4));
            }

            switch (len - pos)
            {
                case 3:
                    ref byte dp2 = ref Unsafe.AddByteOffset(ref destStart, pos + 2);
                    byte bp2 = Unsafe.AddByteOffset(ref sourceStart, pos + 2);
                    dp2 = (byte)(bp2 | Unsafe.AddByteOffset(ref lowerCaseTable, bp2));
                    goto case 2;
                case 2:
                    ref byte dp1 = ref Unsafe.AddByteOffset(ref destStart, pos + 1);
                    byte bp1 = Unsafe.AddByteOffset(ref sourceStart, pos + 1);
                    dp1 = (byte)(bp1 | Unsafe.AddByteOffset(ref lowerCaseTable, bp1));
                    goto case 1;
                case 1:
                    ref byte dp0 = ref Unsafe.AddByteOffset(ref destStart, pos);
                    byte bp0 = Unsafe.AddByteOffset(ref sourceStart, pos);
                    dp0 = (byte)(bp0 | Unsafe.AddByteOffset(ref lowerCaseTable, bp0));
                    goto case 0;
                case 0:
                    break;
            }

            if (tokens != destTokens)
                tokens.CopyTo(destTokens);

            // We need to shrink the tokens and bytes output. 
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }

        public static int ToLowercase(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            return tokens.Length == 1 
                ? ToLowercaseSingleToken<Default>(source, tokens, ref dest, ref destTokens) 
                : ToLowercaseMultipleTokens(source, tokens, ref dest, ref destTokens);
        }
        
        public static int ToLowercasePre24423(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            return ToLowercaseSingleToken<Pre24423>(source, tokens, ref dest, ref destTokens);
        }
        
        private interface IMode{ }
        private record struct Pre24423 : IMode;
        private record struct Default : IMode;
        
        private static int ToLowercaseMultipleTokens(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            int destPos = 0;
            int tokenIdx;

            for (tokenIdx = 0; tokenIdx < tokens.Length; tokenIdx++)
            {
                Token token = tokens[tokenIdx];
                int destStart = destPos;
                int destUsed = 0;
                ReadOnlySpan<byte> sourceSpan = source.Slice(token.Offset, (int)token.Length);
                while (sourceSpan.IsEmpty == false)
                {
                    var opStatus = Rune.DecodeFromUtf8(sourceSpan, out var rune, out int bytesSourceConsumed);
                    if (opStatus != OperationStatus.Done)
                        throw new InvalidDataException($"Invalid UTF8 stream received. Operation Status: {opStatus}");
                    
                    sourceSpan = sourceSpan.Slice(bytesSourceConsumed);

                    rune = ToLowerInvariantLikeLucene(rune);
                    if (rune.TryEncodeToUtf8(dest.Slice(destPos + destUsed), out int bytesWritten) == false)
                        throw new InvalidDataException($"Destination buffer is too small. Buffer Size: {dest.Length}, Write Position: {destPos + destUsed}");
                    
                    destUsed += bytesWritten;
                }
                

                destTokens[tokenIdx] = token with { Offset = destStart, Length = (uint)destUsed };
                destPos += destUsed;
            }
            
            dest = dest.Slice(0, destPos);
            destTokens = destTokens.Slice(0, tokenIdx);
            return source.Length;
        }

        private static int ToLowercaseSingleToken<TMode>(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        where TMode : IMode
        {
            Debug.Assert(typeof(TMode) == typeof(Pre24423) || tokens.Length == 1);
            nint sourcePos = 0;
            nint destPos = 0;
            nint len = source.Length;

            ref byte bufferStart = ref MemoryMarshal.GetReference(source);
            ref byte destStart = ref MemoryMarshal.GetReference(dest);
            ref byte lowerCaseTable = ref MemoryMarshal.GetReference(StandardTransformers.LowercaseTable);

            const int N64 = sizeof(ulong);
            
            while (sourcePos < len)
            {
                while (sourcePos + 2 * N64 < len)
                {
                    ulong v1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)sourcePos));
                    ulong v2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)sourcePos + sizeof(ulong)));

                    // We break out if non-ascii character is found
                    if (((v1 | v2) & ParsingConstants.NonAsciiUInt64Mask) != 0)
                        break;

                    // From here on, we know we are dealing with a codepoint that is ASCII.

                    // We are testing !((ch & CASE_MASK) ^ ('A' & CASE_MASK)) which means character requires transformation.
                    if (((v1 & ParsingConstants.UppercaseUInt64Mask) ^ (0x4141414141414141 & ParsingConstants.UppercaseUInt64Mask)) != ParsingConstants.SpaceCharactersUInt64)
                        goto LOWERCASE_ASCII;

                    Unsafe.WriteUnaligned<ulong>(ref Unsafe.AddByteOffset(ref destStart, destPos), v1);
                    sourcePos += N64;
                    destPos += N64;

                    // We are testing !((ch & CASE_MASK) ^ ('A' & CASE_MASK)) which means character requires transformation.
                    if (((v2 & ParsingConstants.UppercaseUInt64Mask) ^ (0x4141414141414141 & ParsingConstants.UppercaseUInt64Mask)) != ParsingConstants.SpaceCharactersUInt64)
                        goto LOWERCASE_ASCII;

                    Unsafe.WriteUnaligned<ulong>(ref Unsafe.AddByteOffset(ref destStart, destPos), v2);
                    sourcePos += N64;
                    destPos += N64;

                    continue;

                    LOWERCASE_ASCII:
                    
                    // We perform lowercase ASCII using a loop for the ulong we detected needed the lowercase.
                    for (int i = 0; i < 4; i++, sourcePos += 2, destPos += 2)
                    {
                        // dest[pos] = source[pos] | lowercaseTable[source[pos]]
                        ref byte d1 = ref Unsafe.AddByteOffset(ref destStart, destPos);
                        byte b1 = Unsafe.AddByteOffset(ref bufferStart, sourcePos);
                        d1 = (byte)(b1 | Unsafe.AddByteOffset(ref lowerCaseTable, b1));

                        // dest[pos+1] = source[pos+1] | lowercaseTable[source[pos+1]]
                        ref byte d2 = ref Unsafe.AddByteOffset(ref destStart, destPos + 1);
                        byte b2 = Unsafe.AddByteOffset(ref bufferStart, sourcePos + 1);
                        d2 = (byte)(b2 | Unsafe.AddByteOffset(ref lowerCaseTable, b2));
                    }
                }

                if (sourcePos == len)
                    break;

                // We know that now we must lowercase some non-ASCII character OR we don't have enough characters to work
                // in bulk. Either way we will handle this here.
                var opStatus = Rune.DecodeFromUtf8(source.Slice((int)sourcePos), out var rune, out int bytesConsumed);
                if (opStatus != OperationStatus.Done)
                    throw new InvalidDataException($"Invalid UTF8 stream received. Operation Status: {opStatus}");

                rune = ToLowerInvariantLikeLucene(rune);

                // Encode the rune into UTF8
                if (rune.TryEncodeToUtf8(dest.Slice((int)destPos), out int bytesWritten) == false)
                    throw new InvalidDataException($"Destination buffer is too small. Buffer Size: {dest.Length}, Write Position: {destPos}");

                sourcePos += bytesConsumed;
                destPos += bytesWritten;
            }

            if (tokens != destTokens)
                tokens.CopyTo(destTokens);

            // We need to shrink the tokens and bytes output. 
            if (typeof(TMode) == typeof(Default))
            {
                destTokens = destTokens.Slice(0, 1);
                destTokens[0].Length = (uint)destPos;
            }
            else if (typeof(TMode) == typeof(Pre24423))
            {
                destTokens = destTokens.Slice(0, tokens.Length);
            }
            
            dest = dest.Slice(0, (int)destPos);

            return source.Length;
        }
    }
}
