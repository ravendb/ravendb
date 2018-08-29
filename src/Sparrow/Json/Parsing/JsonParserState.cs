using System.Runtime.CompilerServices;
using Sparrow.Collections;

namespace Sparrow.Json.Parsing
{
    public unsafe class JsonParserState
    {
        public const int EscapePositionItemSize = 5;
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
            
            for (int i = 0; i < str.Length; i++)
            {
                byte value = (byte)str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 13 => '\r' => 0000 1101
                // 10 => '\n' => 0000 1010
                // 12 => '\f' => 0000 1100
                // 34 => '\\' => 0010 0010
                // 92 =>  '"' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                    count++;
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize; 
        }

        public void FindEscapePositionsIn(byte* str, int len, int previousComputedMaxSize)
        {
            FindEscapePositionsIn(EscapePositions, str, len, previousComputedMaxSize);
        }

        public static void FindEscapePositionsIn(FastList<int> buffer, byte* str, int len, int previousComputedMaxSize)
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
                }
            }
        }

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
    }
}
