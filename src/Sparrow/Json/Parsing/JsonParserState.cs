using System.Runtime.CompilerServices;
using Sparrow.Collections;

namespace Sparrow.Json.Parsing
{
    public unsafe class JsonParserState
    {
        public const int EscapePositionItemSize = 5;
        public const int ControlCharacterItemSize = 5;
        public byte* StringBuffer;
        public int StringSize;
        public int? CompressedSize;
        public long Long;
        public JsonParserToken CurrentTokenType;
        public JsonParserTokenContinuation Continuation;

        public readonly FastList<int> EscapePositions = new FastList<int>();
       
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteVariableSizeInt(ref byte* dest, int value)
        {
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                *dest++ = (byte)(v | 0x80);
                v >>= 7;
            }
            *dest++ = (byte)(v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int VariableSizeIntSize(int value)
        {
            int count = 0;
            // assume that we don't use negative values very often
            var v = (uint)value;
            while (v >= 0x80)
            {
                v >>= 7;
                count++;
            }
            count++;
            return count;
        }

        public static int FindEscapePositionsMaxSize(string str)
        {
            var count = 0;
            var controlCount = 0;
            
            for (int i = 0; i < str.Length; i++)
            {
                char value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 13 => '\r' => 0000 1101
                // 10 => '\n' => 0000 1010
                // 12 => '\f' => 0000 1100
                // 34 => '\\' => 0010 0010
                // 92 =>  '"' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32 || value >= 127 && value <= 159)
                {
                    controlCount++;
                }

            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapePositionsIn(byte* str, ref int len, int previousComputedMaxSize)
        {
            FindEscapePositionsIn(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        public static void FindEscapePositionsIn(FastList<int> buffer, byte* str, ref int len, int previousComputedMaxSize)
        {
            buffer.Clear();
            if (previousComputedMaxSize == EscapePositionItemSize)
            {
                // if the value is 5, then we got no escape positions, see: FindEscapePositionsMaxSize
                // and we don't have to do any work
                return;
            }

            var lastEscape = 0;
            for (int i = 0; i < len; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 13 => '\r' => 0000 1101
                // 10 => '\n' => 0000 1010
                // 12 => '\f' => 0000 1100
                // 34 => '\\' => 0010 0010
                // 92 =>  '"' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    buffer.Add(i - lastEscape);
                    lastEscape = i + 1;
                    continue;
                }

                //Control character ascii values
                if (value < 32 || value >= 127 && value <= 159)
                {
                    // move rest of buffer 
                    // write \u0000
                    // update size
                    var from = str + i + 1;
                    var to = str + i + 1 + SizeOfUtf8AsStringInBytesWithoutEscaping;
                    var sizeToCopy = len - i;
                    //here we only shifting by 5 bytes since we are going to override the byte at the current position.
                    Memory.Copy(to, from, sizeToCopy ); //TODO: we must verify that we have enough space in our buffer otherwise we must request a bigger one
                    //TODO: find a way to generate the table as pinned
                    fixed (byte* controlString = EscapeCharactersStringsAsBytes[value])
                    {
                        Memory.Copy(str + i, controlString, SizeOfUtf8AsStringInBytesWithoutEscaping + 1);
                    }
                    //The original string already had one byte so we only added 5.
                    len += SizeOfUtf8AsStringInBytesWithoutEscaping;
                }
            }
        }

        private static readonly int SizeOfUtf8AsStringInBytesWithoutEscaping = 5;

        public int WriteEscapePositionsTo(byte* buffer)
        {
            var escapePositions = EscapePositions;
            var originalBuffer = buffer;
            WriteVariableSizeInt(ref buffer, escapePositions.Count);

            // PERF: Using a for in this way will evict the bounds-check and also avoid the cost of using an struct enumerator. 
            for (int i = 0; i < escapePositions.Count; i++)
                WriteVariableSizeInt(ref buffer, escapePositions[i]);

            return (int)(buffer - originalBuffer);
        }

        public void Reset()
        {
            StringBuffer = null;
            StringSize = 0;
            CompressedSize = null;
            Long = 0;
            CurrentTokenType = JsonParserToken.None;
            Continuation = JsonParserTokenContinuation.None;
            EscapePositions.Clear();
        }

        //This table was programaticly generated 
        public static readonly byte[][] EscapeCharactersStringsAsBytes = new byte[160][]
        {
            new byte[]{92,117,48,48,48,48},new byte[]{92,117,48,48,48,49},new byte[]{92,117,48,48,48,50},new byte[]{92,117,48,48,48,51},new byte[]{92,117,48,48,48,52},new byte[]{92,117,48,48,48,53},
            new byte[]{92,117,48,48,48,54},new byte[]{92,117,48,48,48,55},new byte[]{92,117,48,48,48,56},new byte[]{92,117,48,48,48,57},new byte[]{92,117,48,48,48,65},new byte[]{92,117,48,48,48,66},
            new byte[]{92,117,48,48,48,67},new byte[]{92,117,48,48,48,68},new byte[]{92,117,48,48,48,69},new byte[]{92,117,48,48,48,70},new byte[]{92,117,48,48,49,48},new byte[]{92,117,48,48,49,49},
            new byte[]{92,117,48,48,49,50},new byte[]{92,117,48,48,49,51},new byte[]{92,117,48,48,49,52},new byte[]{92,117,48,48,49,53},new byte[]{92,117,48,48,49,54},new byte[]{92,117,48,48,49,55},
            new byte[]{92,117,48,48,49,56},new byte[]{92,117,48,48,49,57},new byte[]{92,117,48,48,49,65},new byte[]{92,117,48,48,49,66},new byte[]{92,117,48,48,49,67},new byte[]{92,117,48,48,49,68},
            new byte[]{92,117,48,48,49,69},new byte[]{92,117,48,48,49,70},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},new byte[]{},
            new byte[]{},new byte[]{92,117,48,48,55,70},new byte[]{92,117,48,48,56,48},new byte[]{92,117,48,48,56,49},new byte[]{92,117,48,48,56,50},new byte[]{92,117,48,48,56,51},
            new byte[]{92,117,48,48,56,52},new byte[]{92,117,48,48,56,53},new byte[]{92,117,48,48,56,54},new byte[]{92,117,48,48,56,55},new byte[]{92,117,48,48,56,56},new byte[]{92,117,48,48,56,57},
            new byte[]{92,117,48,48,56,65},new byte[]{92,117,48,48,56,66},new byte[]{92,117,48,48,56,67},new byte[]{92,117,48,48,56,68},new byte[]{92,117,48,48,56,69},new byte[]{92,117,48,48,56,70},
            new byte[]{92,117,48,48,57,48},new byte[]{92,117,48,48,57,49},new byte[]{92,117,48,48,57,50},new byte[]{92,117,48,48,57,51},new byte[]{92,117,48,48,57,52},new byte[]{92,117,48,48,57,53},
            new byte[]{92,117,48,48,57,54},new byte[]{92,117,48,48,57,55},new byte[]{92,117,48,48,57,56},new byte[]{92,117,48,48,57,57},new byte[]{92,117,48,48,57,65},new byte[]{92,117,48,48,57,66},
            new byte[]{92,117,48,48,57,67},new byte[]{92,117,48,48,57,68},new byte[]{92,117,48,48,57,69},new byte[]{92,117,48,48,57,70}
        };
    }
}
