using System;
using System.Runtime.CompilerServices;
using Sparrow.Collections;

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

        //RavenDB-25738 For backward compatibility only. Use only for collection, attachment name & type, and timeseries tag
        public static int FindMaxEscapePositionAndControlCharSize(ReadOnlySpan<char> str, out int controlCount)
        {
            var count = 0;
            controlCount = 0;

            for (int i = 0; i < str.Length; i++)
            {
                var value = str[i];
                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 10 => '\n' => 0000 1010

                // 12 => '\f' => 0000 1100
                // 13 => '\r' => 0000 1101

                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32)
                {
                    controlCount++;
                }
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
        }
        
        //RavenDB-25738 For backward compatibility only. Use only for collection, attachment name & type, and timeseries tag
        public static int FindMaxEscapePositionAndControlCharSize(byte* str, int size, out int escapedCount)
        {
            var count = 0;
            var controlCount = 0;

            for (int i = 0; i < size; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 8  => '\b' => 0000 1000
                // 9  => '\t' => 0000 1001
                // 10 => '\n' => 0000 1010

                // 12 => '\f' => 0000 1100
                // 13 => '\r' => 0000 1101

                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                {
                    count++;
                    continue;
                }

                if (value < 32)
                {
                    controlCount++;
                }
            }

            escapedCount = controlCount;
            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
        }
        
        public static int FindMaxEscapePositionSize(ReadOnlySpan<char> str)
        {
            var count = 0;
            for (int i = 0; i < str.Length; i++)
            {
                var value = str[i];
                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value < 32 || value == 92 || value == 34)
                    count++;
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize;
        }

        public static int FindMaxEscapePosition(byte* str, int size)
        {
            var count = 0;
            for (int i = 0; i < size; i++)
            {
                byte value = str[i];

                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value < 32 || value == 92 || value == 34)
                    count++;
            }

            // we take 5 because that is the max number of bytes for variable size int
            // plus 1 for the actual number of positions

            // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
            return (count + 1) * EscapePositionItemSize;
        }

        //RavenDB-25738 For backward compatibility only. Use only for collection, attachment name & type, and timeseries tag
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositionsAndEscapeControls(byte* str, ref int len, int previousComputedMaxSize)
        {
            FindEscapedPositionsAndEscapeControls(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        //RavenDB-25738 For backward compatibility only. Use only for collection, attachment name & type, and timeseries tag
        private static void FindEscapedPositionsAndEscapeControls(FastList<int> buffer, byte* str, ref int len, int previousComputedMaxSize)
        {
            var originalLen = len;
            buffer.Clear();
            if (previousComputedMaxSize == EscapePositionItemSize)
            {
                // if the value is 5, then we got no escape positions, see: FindMaxEscapePositionAndControlCharSize
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
                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100

                if (value == 92 || value == 34 || (value is >= 8 and <= 13 && value != 11))
                {
                    buffer.Add(i - lastEscape);
                    lastEscape = i + 1;
                    continue;
                }
                //Control character ascii values
                if (value < 32)
                {
                    if (len + ControlCharacterItemSize > originalLen + previousComputedMaxSize)
                        ThrowInvalidSizeForEscapeControlChars(previousComputedMaxSize);

                    // move rest of buffer 
                    // write \u0000
                    // update size
                    var from = str + i + 1;
                    var to = str + i + 1 + ControlCharacterItemSize;
                    var sizeToCopy = len - i - 1;
                    //here we only shifting by 5 bytes since we are going to override the byte at the current position.
                    // source and destination blocks may overlap so we using Buffer.MemoryCopy to handle that scenario.
                    Buffer.MemoryCopy(from, to, (uint)sizeToCopy, (uint)sizeToCopy);
                    str[i] = (byte)'\\';
                    str[i + 1] = (byte)'u';
                    *(int*)(str + i + 2) = AbstractBlittableJsonTextWriter.ControlCodeEscapes[value];
                    //The original string already had one byte so we only added 5.
                    len += ControlCharacterItemSize;
                    i += ControlCharacterItemSize;
                }
            }
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FindEscapedPositions(byte* str, ref int len, int previousComputedMaxSize)
        {
            FindEscapedPositions(EscapePositions, str, ref len, previousComputedMaxSize);
        }

        public static void FindEscapedPositions(FastList<int> buffer, byte* str, ref int len, int previousComputedMaxSize)
        {
            buffer.Clear();
            if (previousComputedMaxSize == EscapePositionItemSize)
            {
                // if the value is 5, then we got no escape positions, see: FindMaxEscapePosition
                // and we don't have to do any work
                return;
            }

            var lastEscape = 0;
            for (int i = 0; i < len; i++)
            {
                // PERF: We use the values directly because it is 5x faster than iterating over a constant array.
                // 34 => '"'  => 0010 0010
                // 92 => '\\' => 0101 1100
                if (str[i] is not (92 or 34 or < 32)) 
                    continue;
                
                buffer.Add(i - lastEscape);
                lastEscape = i + 1;
            }
        }
        
        private static void ThrowInvalidSizeForEscapeControlChars(int previousComputedMaxSize)
        {
            throw new InvalidOperationException($"The previousComputedMaxSize: {previousComputedMaxSize} is too small to support the required escape positions. Did you not call FindMaxNumberOfEscapePositions?");
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
