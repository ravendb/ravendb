using System.Collections.Generic;

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

        public readonly List<int> EscapePositions = new List<int>();

        public static readonly char[] EscapeChars = { '\b', '\t', '\r', '\n', '\f', '\\', '"' };
        public static readonly byte[] EscapeCharsAsBytes = { (byte)'\b', (byte)'\t', (byte)'\r', (byte)'\n', (byte)'\f', (byte)'\\', (byte)'"' };


        public int GetEscapePositionsSize()
        {
            return GetEscapePositionsSize(EscapePositions);
        }

        public static int GetEscapePositionsSize(List<int> escapePosiitons)
        {
            int size = VariableSizeIntSize(escapePosiitons.Count);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (int pos in escapePosiitons)
            {
                size += VariableSizeIntSize(pos);
            }
            return size;
        }


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

        public int FindEscapePositionsMaxSize(string str)
        {
            var count = 0;
            var lastEscape = 0;
            while (true)
            {
                var curEscape = str.IndexOfAny(EscapeChars, lastEscape);
                if (curEscape == -1)
                    break;

                count++;
                lastEscape = curEscape + 1;
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * 5; 
        }

        public void FindEscapePositionsIn(byte* str, int len, int previousComputedMaxSize)
        {
            EscapePositions.Clear();
            if (previousComputedMaxSize == 5)
            {
                // if the value is 5, then we got no escape positions, see: FindEscapePositionsMaxSize
                // and we don't have to do any work
                return;
            }
            var lastEscape = 0;
            for (int i = 0; i < len; i++)
            {
                for (int j = 0; j < EscapeCharsAsBytes.Length; j++)
                {
                    if (str[i] == EscapeCharsAsBytes[j])
                    {
                        EscapePositions.Add(i - lastEscape);
                        lastEscape = i + 1;
                        break;
                    }
                }
            }
        }

        public void WriteEscapePositionsTo(byte* buffer)
        {
            WriteVariableSizeInt(ref buffer, EscapePositions.Count);
            foreach (int pos in EscapePositions)
            {
                WriteVariableSizeInt(ref buffer, pos);
            }
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