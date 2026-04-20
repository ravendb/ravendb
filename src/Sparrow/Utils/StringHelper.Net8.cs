#if NET8_0_OR_GREATER
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Sparrow.Utils;

public static partial class StringUtils
{
    private static readonly SearchValues<byte> ControlCharactersAsBytes;
    private static readonly SearchValues<char> ControlCharacters;

    private static readonly SearchValues<char> EscapeCharacters;

    static StringUtils()
    {
        var controlCharacters = BuildEscapeCharacters(onlyControlCharacters: true);
        ControlCharactersAsBytes = SearchValues.Create(controlCharacters);
        ControlCharacters = SearchValues.Create(controlCharacters.Select(x => (char)x).ToArray());

        var escapedCharacters = BuildEscapeCharacters(onlyControlCharacters: false);
        EscapeCharacters = SearchValues.Create(escapedCharacters.Select(x => (char)x).ToArray());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HasControlCharacters(ReadOnlySpan<byte> str) => str.IndexOfAny(ControlCharactersAsBytes) != -1;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HasControlCharacters(ReadOnlySpan<char> str) => str.IndexOfAny(ControlCharacters) != -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int FindMaxEscapePositionSize(ReadOnlySpan<char> str)
    {
        // Fast path: SIMD scan to find the first escape character.
        // For the common case of no escape characters this returns immediately.
        int firstIndex = str.IndexOfAny(EscapeCharacters);
        if (firstIndex == -1)
            return EscapePositionItemSize;

        // At least one escape character found - fall back to linear scan.
        var count = CountEscapeChars(str[(firstIndex + 1)..]) + 1;
        return (count + 1) * EscapePositionItemSize;
    }
    
    private static byte[] BuildEscapeCharacters(bool onlyControlCharacters)
    {
        // Escape characters that are valid inside JSON
        // 8  => '\b' => 0000 1000
        // 9  => '\t' => 0000 1001
        // 10 => '\n' => 0000 1010
        // 12 => '\f' => 0000 1100
        // 13 => '\r' => 0000 1101

        List<byte> ret = new List<byte>();
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
}
#endif
