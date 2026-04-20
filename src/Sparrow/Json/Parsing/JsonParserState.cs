using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Utils;

namespace Sparrow.Json.Parsing
{
    public sealed unsafe class JsonParserState
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

        //RavenDB-25738 For backward compatibility only. Use only for document IDs, collection, attachment name & type, and timeseries tag
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(byte* str, ref int len, int previousComputedMaxSize)
        {
            StringUtils.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositions(byte* str, int len, int previousComputedMaxSize) => StringUtils.FindEscapedPositions(EscapePositions, str, len, previousComputedMaxSize);

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
