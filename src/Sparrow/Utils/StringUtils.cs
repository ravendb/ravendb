using System;
using System.Runtime.CompilerServices;
using Sparrow.Collections;

namespace Sparrow.Utils;

public static unsafe partial class StringUtils
{
    private const int EscapePositionItemSize = 5;

#if !NET8_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FindEscapedPositionsInternal(FastList<int> buffer, byte* str, int len)
        => FindEscapedPositionsLinearSearchInternal(buffer, str, len);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeChars(byte* str, int size) => CountEscapeCharsLinearSearch(str, size);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeChars(ReadOnlySpan<char> str) => CountEscapeCharsLinearSearch(str);
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsControlCharacter(char c) => c < 8 || c > 13 && c < 32 || c == 11;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void FindEscapedPositions(FastList<int> buffer, byte* str, int len, int previousComputedMaxSize)
    {
        buffer.Clear();
        if (previousComputedMaxSize == EscapePositionItemSize)
        {
            // if the value is 5, then we got no escape positions, see: FindMaxEscapePositionSize
            // and we don't have to do any work
            return;
        }

        FindEscapedPositionsInternal(buffer, str, len);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FindMaxEscapePositionSize(byte* str, int size)
    {
        int count = CountEscapeChars(str, size);

        // we take 5 because that is the max number of bytes for variable size int
        // plus 1 for the actual number of positions

        // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
        return (count + 1) * EscapePositionItemSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FindMaxEscapePositionSize(ReadOnlySpan<char> str)
    {
        var count = CountEscapeChars(str);
        // NOTE: this is used by FindEscapePositionsIn, change only if you also modify FindEscapePositionsIn
        return (count + 1) * EscapePositionItemSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FindEscapedPositionsLinearSearchInternal(FastList<int> buffer, byte* str, int len)
    {
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeCharsLinearSearch(byte* str, int size)
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

        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeCharsLinearSearch(ReadOnlySpan<char> str)
    {
        var count = 0;
        for (var i = 0; i < str.Length; i++)
        {
            var value = str[i];
            // 34 => '"'  => 0010 0010
            // 92 => '\\' => 0101 1100
            if (value < 32 || value == 92 || value == 34)
                count++;
        }
        return count;
    }
}
