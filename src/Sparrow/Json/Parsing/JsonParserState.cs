using System.Runtime.CompilerServices;
using Sparrow.Collections;

namespace Sparrow.Json.Parsing
{
    public unsafe class JsonParserState
    {
        public byte* StringBuffer;
        public int StringSize;
        public int? CompressedSize;
        public long Long;
        public JsonParserToken CurrentTokenType;
        public JsonParserTokenContinuation Continuation;

        public readonly FastList<int> EscapePositions = new FastList<int>();

        public static readonly char[] EscapeChars = { '\b', '\t', '\r', '\n', '\f', '\\', '"' };
        public static readonly byte[] EscapeCharsAsBytes = { (byte)'\b', (byte)'\t', (byte)'\r', (byte)'\n', (byte)'\f', (byte)'\\', (byte)'"' };
       
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
            
            var escapeCharsAsBytes = EscapeCharsAsBytes;
            int escapeCharsLength = escapeCharsAsBytes.Length;

            for (int i = 0; i < str.Length; i++)
            {
                for (int j = 0; j < escapeCharsLength; j++)
                {
                    if (str[i] == escapeCharsAsBytes[j])
                    {
                        count++;
                        break;
                    }
                }
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5; 
        }

        public void FindEscapePositionsIn(byte* str, int len, int previousComputedMaxSize)
        {
            FindEscapePositionsIn(EscapePositions, str, len, previousComputedMaxSize);
        }

        public static void FindEscapePositionsIn(FastList<int> buffer, byte* str, int len, int previousComputedMaxSize)
        {
            buffer.Clear();
            if (previousComputedMaxSize == 5)
            {
                // if the value is 5, then we got no escape positions, see: FindEscapePositionsMaxSize
                // and we don't have to do any work
                return;
            }
            var lastEscape = 0;
            var escapeCharsAsBytes = EscapeCharsAsBytes;
            int escapeCharsLength = escapeCharsAsBytes.Length;

            for (int i = 0; i < len; i++)
            {
                for (int j = 0; j < escapeCharsLength; j++)
                {
                    if (str[i] == escapeCharsAsBytes[j])
                    {
                        buffer.Add(i - lastEscape);
                        lastEscape = i + 1;
                        break;
                    }
                }
            }
        }

        public void WriteEscapePositionsTo(byte* buffer)
        {
            var escapePositions = EscapePositions;

            WriteVariableSizeInt(ref buffer, escapePositions.Count);

            // PERF: Using a for in this way will evict the bounds-check and also avoid the cost of using an struct enumerator. 
            for (int i = 0; i < escapePositions.Count; i++)
                WriteVariableSizeInt(ref buffer, escapePositions[i]);
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