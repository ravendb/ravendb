﻿using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.InteropServices.Marshalling;
using System.Runtime.Intrinsics.X86;
using System.Text.Unicode;


namespace Corax.Pipeline.Parsing
{
    internal static class StandardTransformers
    {
        internal static ReadOnlySpan<byte> LowercaseTable => new byte[]
        {
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 00 - 0F
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 - 1F
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 20 - 2F
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 30 - 3F
            0x00, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, // 40 - 4F
            0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, // 50 - 5F
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 60 - 6F
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 70 - 7F
        };

        public static int ToLowercaseAscii(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            Debug.Assert(source.Length <= dest.Length);
            Debug.Assert(source.Length <= destTokens.Length);

            return ScalarTransformers.ToLowercaseAscii(source, tokens, ref dest, ref destTokens);
        }

        public static int ToLowercase(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            Debug.Assert(source.Length <= dest.Length);
            Debug.Assert(source.Length <= destTokens.Length);

            return ScalarTransformers.ToLowercase(source, tokens, ref dest, ref destTokens);
        }

        public static int ToLowercase(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
        {
            Debug.Assert(source.Length <= dest.Length);
            Debug.Assert(source.Length <= destTokens.Length);

            return ScalarTransformers.ToLowercase(source, tokens, ref dest, ref destTokens);
        }
    }
}
