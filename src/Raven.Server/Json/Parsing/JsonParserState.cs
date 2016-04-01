using System;
using System.Collections.Generic;
using Raven.Server.Utils;

namespace Raven.Server.Json.Parsing
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

        private static readonly char[] EscapeChars = { '\b', '\t', '\r', '\n', '\f', '\\', '/', '"', };

        public int GetEscapePositionsSize()
        {
            int size = VariableSizeIntSize(EscapePositions.Count);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (int pos in EscapePositions)
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

        public void FindEscapePositionsIn(string str)
        {
            FindEscapePositionsIn(new StringSegment(str,0,str.Length));				
        }

        public void FindEscapePositionsIn(StringSegment str)
        {
            EscapePositions.Clear();
            var lastEscape = 0;
            while (true)
            {
                var curEscape = str.IndexOfAny(EscapeChars, lastEscape);
                if (curEscape == -1)
                    break;
                EscapePositions.Add(curEscape - lastEscape);
                lastEscape = curEscape + 1;
            }
        }


        public void FindEscapePositionsIn(char[] buffer, int start, int count)
        {
            EscapePositions.Clear();
            //TODO: inefficient, need to copy  COMString::IndexOfCharArray
            var lastEscape = 0;
            while (true)
            {
                var curEscape = -1;

                int offset = start + lastEscape;
                foreach (var escapeChar in EscapeChars)
                {
                    curEscape = Array.IndexOf(buffer, escapeChar, start + offset, count - offset);
                    if (curEscape != -1)
                        break;
                }

                if (curEscape == -1)
                    break;
                EscapePositions.Add(curEscape - lastEscape);
                lastEscape = curEscape + 1;
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
    }
}