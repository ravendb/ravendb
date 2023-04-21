using System;

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

    [Flags]
    public enum TokenType : uint
    {
        None = 0,
        Word = 1,
        Term = 2,

        Ascii = 4,
        Numeric = 8,        
        Alphabetic = 9,
        Alphanumeric = Numeric | Alphabetic,        
        UserDefined = 0x8000_0000,
        Invalid = 0xFFFF_FFFF,
    }
}
