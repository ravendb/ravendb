using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline.Parsing
{
    internal static class ParsingConstants
    {
        // U+0020  space 
        public const byte SpaceCharacter = 0x20;

        // U+0080  first non-ascii character in the table, it also works as a mask to detect non-ascii characters.
        public const byte NonAsciiMask = 0x80;

        // A mask that allow us to isolate the only non-ascii bit in every byte inside a packed 4 bytes word. 
        public const uint NonAsciiUInt32Mask = 0x80808080;

        // A mask that allow us to isolate the only non-ascii bit in every byte inside a packed 8 bytes word. 
        public const ulong NonAsciiUInt64Mask = 0x8080808080808080;

        // The mask that allow us to detect uppercase characters in the ascii realm. 
        public const byte UppercaseMask = 'A' ^ 'a';

        // The mask that allow us to detect uppercase characters inside a packed 4 bytes word. 
        public const uint UppercaseUInt32Mask = UppercaseMask | UppercaseMask << 8 | UppercaseMask << 16 | UppercaseMask << 24;

        // The mask that allow us to detect uppercase characters inside a packed 8 bytes word. 
        public const ulong UppercaseUInt64Mask = (ulong)UppercaseUInt32Mask << 32 | UppercaseUInt32Mask;

        // U+0009  character tabulation
        // U+000A  line feed
        // U+000B  line tabulation
        // U+000C  form feed
        // U+000D  carriage return
        // U+001C  file separator
        // U+001D  group separator
        // U+001E  record separator
        // U+001F  unit separator
        // U+0020  space

        // PERF: These tables are bitmaps that mark with 1 whenever a character in the ASCII range between 0 and 63 can be
        // considered as a whitespace. By checking if the bit is 1 or 0 which only involves a MOV, a SHIFT and a AND assembler
        // opcode, and then we can verify this property without any branches. 
        internal const long SingleByteWhitespaceTable =
            1L << 0x09 | 1L << 0x0A | 1L << 0x0B | 1L << 0x0C | 1L << 0x0D |
            1L << 0x1C | 1L << 0x1D | 1L << 0x1E | 1L << 0x1F | 1L << 0x20;

        // U+2000  en quad
        // U+2001  em quad
        // U+2002  en space
        // U+2003  em space
        // U+2004  three-per-em space
        // U+2005  four-per-em space
        // U+2006  six-per-em space
        // U+2007  figure space
        // U+2008  punctuation space
        // U+2009  thin space
        // U+200A  hair space
        // U+200B  zero width space
        // U+200C  zero width non-joiner
        // U+200D  zero width joiner
        // U+2028  line separator
        // U+2029  paragraph separator
        // U+202F  narrow no-break space

        // PERF: These tables are bitmaps that mark with 1 whenever a character in the ASCII range between the U+2000 and U+202F can be
        // considered as a whitespace. By knowing that the first byte is 20 and the value is between 0 and 64 we can check if the bit
        // is 1 or 0 which only involves a MOV, a SHIFT and a AND assembler opcode, and then we can verify this property without any branches. 
        internal const long SecondByte20WhitespaceTable =
            1L << 0x00 | 1L << 0x01 | 1L << 0x02 | 1L << 0x03 | 1L << 0x04 | 1L << 0x05 | 1L << 0x06 | 1L << 0x07 |
            1L << 0x08 | 1L << 0x09 | 1L << 0x0A | 1L << 0x0B | 1L << 0x0C | 1L << 0x0D | 1L << 0x28 | 1L << 0x29 |
            1L << 0x2F;
    }
}
