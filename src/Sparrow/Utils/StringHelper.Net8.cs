#if NET8_0_OR_GREATER
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Json;

namespace Sparrow.Utils;

public static partial class StringUtils
{
    private const int ControlCharacterItemSize = 5;

    private static readonly SearchValues<byte> ControlCharactersAsBytes;
    private static readonly SearchValues<char> ControlCharacters;

    private static readonly SearchValues<char> EscapeCharacters;
    private static readonly SearchValues<byte> EscapeCharactersAsBytes;

    static StringUtils()
    {
        var controlCharacters = BuildEscapeCharacters(onlyControlCharacters: true);
        ControlCharactersAsBytes = SearchValues.Create(controlCharacters);
        ControlCharacters = SearchValues.Create(controlCharacters.Select(x => (char)x).ToArray());

        var escapedCharacters = BuildEscapeCharacters(onlyControlCharacters: false);
        EscapeCharacters = SearchValues.Create(escapedCharacters.Select(x => (char)x).ToArray());
        EscapeCharactersAsBytes = SearchValues.Create(escapedCharacters);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HasControlCharacters(ReadOnlySpan<byte> str) => str.IndexOfAny(ControlCharactersAsBytes) != -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HasControlCharacters(ReadOnlySpan<char> str) => str.IndexOfAny(ControlCharacters) != -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe int FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(byte* str, int size, out int escapedCount)
    {
        var count = 0;
        var controlCount = 0;
        var remaining = new ReadOnlySpan<byte>(str, size);
        while (true)
        {
            int idx = remaining.IndexOfAny(EscapeCharactersAsBytes);
            if (idx == -1)
                break;
            byte value = remaining[idx];
            if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                count++;
            else
                controlCount++;
            remaining = remaining[(idx + 1)..];
        }
        escapedCount = controlCount;
        return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(ReadOnlySpan<char> str, out int controlCount)
    {
        var count = 0;
        controlCount = 0;
        while (true)
        {
            int idx = str.IndexOfAny(EscapeCharacters);
            if (idx == -1)
                break;
            var value = str[idx];
            if (value == 92 || value == 34 || (value >= 8 && value <= 13 && value != 11))
                count++;
            else
                controlCount++;
            str = str[(idx + 1)..];
        }
        return (count + 1) * EscapePositionItemSize + controlCount * ControlCharacterItemSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(FastList<int> buffer, byte* str, ref int len, int previousComputedMaxSize)
    {
        var originalLen = len;
        buffer.Clear();
        if (previousComputedMaxSize == EscapePositionItemSize)
        {
            // if the value is 5, then we got no escape positions, see: FindMaxEscapePositionSize
            // and we don't have to do any work
            return;
        }

        var lastEscape = 0;
        var i = 0;
        while (i < len)
        {
            var remaining = new ReadOnlySpan<byte>(str + i, len - i);
            int idx = remaining.IndexOfAny(EscapeCharactersAsBytes);
            if (idx == -1)
                break;

            i += idx;
            byte value = str[i];

            if (value == 92 || value == 34 || (value is >= 8 and <= 13 && value != 11))
            {
                buffer.Add(i - lastEscape);
                lastEscape = i + 1;
                i++;
                continue;
            }

            // Control character: expand in-place to \uXXXX
            if (len + ControlCharacterItemSize > originalLen + previousComputedMaxSize)
                ThrowInvalidSizeForEscapeControlChars(previousComputedMaxSize);

            var from = str + i + 1;
            var to = str + i + 1 + ControlCharacterItemSize;
            var sizeToCopy = len - i - 1;
            Buffer.MemoryCopy(from, to, (uint)sizeToCopy, (uint)sizeToCopy);
            str[i] = (byte)'\\';
            str[i + 1] = (byte)'u';
            *(int*)(str + i + 2) = AbstractBlittableJsonTextWriter.ControlCodeEscapes[value];
            len += ControlCharacterItemSize;
            i += ControlCharacterItemSize + 1;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void FindEscapedPositionsInternal(FastList<int> buffer, byte* str, int len)
    {
        var remaining = new ReadOnlySpan<byte>(str, len);
        while (true)
        {
            int idx = remaining.IndexOfAny(EscapeCharactersAsBytes);
            if (idx == -1)
                break;
            buffer.Add(idx);
            remaining = remaining[(idx + 1)..];
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe int CountEscapeChars(byte* str, int size) => CountEscapeChars(new ReadOnlySpan<byte>(str, size));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeChars(ReadOnlySpan<byte> str)
    {
        int count = 0;
        while (true)
        {
            int idx = str.IndexOfAny(EscapeCharactersAsBytes);
            if (idx == -1)
                break;
            count++;
            str = str[(idx + 1)..];
        }
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountEscapeChars(ReadOnlySpan<char> str)
    {
        int count = 0;
        while (true)
        {
            int idx = str.IndexOfAny(EscapeCharacters);
            if (idx == -1)
                break;
            count++;
            str = str[(idx + 1)..];
        }
        return count;
    }

    private static byte[] BuildEscapeCharacters(bool onlyControlCharacters)
    {
        // Escape characters that are valid inside JSON
        // 8  => '\b' => 0000 1000
        // 9  => '\t' => 0000 1001
        // 10 => '\n' => 0000 1010
        // 12 => '\f' => 0000 1100
        // 13 => '\r' => 0000 1101

        var ret = new List<byte>();
        for (var i = 0; i < 8; i++)
            ret.Add((byte)i);

        if (onlyControlCharacters)
        {
            ret.Add((byte)'\v');
        }
        else
        {
            for (var i = 8; i < 14; i++)
                ret.Add((byte)i);

            ret.Add(34);
            ret.Add(92);
        }
        for (var i = 14; i < 32; i++)
            ret.Add((byte)i);

        return ret.ToArray();
    }

    private static void ThrowInvalidSizeForEscapeControlChars(int previousComputedMaxSize)
    {
        throw new InvalidOperationException($"The previousComputedMaxSize: {previousComputedMaxSize} is too small to support the required escape positions. Did you not call FindMaxNumberOfEscapePositions?");
    }
}
#endif
