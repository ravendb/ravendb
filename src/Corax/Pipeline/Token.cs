using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;

namespace Corax.Pipeline
{
    // A Token is a representation of a token data that is contiguous in memory and therefore can be accessed directly and efficiently. 
    // Transformers may change positions, lengths, types and buffers; but need to return a new Token in the process.
    // If the tokenizer, analyzer or filter would not change any of those, they could be reused. 
    public struct Token
    {
        public int Offset;
        public uint Length;
        public TokenType Type;
    }

    public static class TokenTypes
    {
        public const uint None = 0;
        public const uint Word = 1;
        public const uint Keyword = 2;
        public const uint UserDefined = 0x8000_0000;
        public const uint Invalid = 0xFFFF_FFFF;
    }

    [Flags]
    public enum TokenType : uint
    {
        None = TokenTypes.None,
        Word = TokenTypes.Word,
        Keyword = TokenTypes.Keyword,
        UserDefined = TokenTypes.UserDefined,
        Invalid = TokenTypes.Invalid,
    }
}
