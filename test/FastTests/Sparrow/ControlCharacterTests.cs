using System;
using System.Text;
using Sparrow.Collections;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow;

public unsafe class ControlCharacterTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private enum CharType
    {
        None = 0,
        Escape = 1,
        Control = 2,
    }

    private readonly record struct CharCase(char Value, CharType Type, string EscapeSequence = null);

    private static readonly CharCase[] Chars =
    [
        new((char)0,  CharType.Control,     @"\u0000"),
        new((char)1,  CharType.Control,     @"\u0001"),
        new((char)7,  CharType.Control,     @"\u0007"),
        new((char)8,  CharType.Escape),             // \b
        new((char)9,  CharType.Escape),             // \t
        new((char)10, CharType.Escape),             // \n
        new((char)11, CharType.Control,     @"\u000B"),  // \v — sits in 8-13 but is a control char
        new((char)12, CharType.Escape),             // \f
        new((char)13, CharType.Escape),             // \r
        new((char)14, CharType.Control,     @"\u000E"),
        new((char)31, CharType.Control,     @"\u001F"),
        new((char)34, CharType.Escape),             // "
        new((char)92, CharType.Escape),             // \
        new((char)32, CharType.None),
        new((char)65, CharType.None),                    // A
    ];

    // ── IsControlCharacter ──────────────────────────────────────────────────

    public static TheoryData<int, bool> IsControlCharacterData
    {
        get
        {
            var data = new TheoryData<int, bool>();
            foreach (var c in Chars)
            {
                data.Add(c.Value, c.Type == CharType.Control);
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(IsControlCharacterData))]
    public void IsControlCharacter_Char(int value, bool expected) => Assert.Equal(expected, StringUtils.IsControlCharacter((char)value));

    // ── FindEscapedPositions ─────────────────────────────────────────────────
    //
    // Buffer stores relative offsets between successive escape chars.
    // Chars < 32, '"', and '\' all contribute an entry.

    public static TheoryData<string, int[]> FindEscapedPositionsOffsetData
    {
        get
        {
            var data = new TheoryData<string, int[]>();
            foreach (var c in Chars)
            {
                var isEsc = c.Type != CharType.None;
                data.Add($"{c.Value}",       isEsc ? [0]    : []);
                data.Add($"{c.Value}a",      isEsc ? [0]    : []);
                data.Add($"a{c.Value}a",     isEsc ? [1]    : []);
                data.Add($"a{c.Value}",      isEsc ? [1]    : []);
                data.Add($"a{c.Value}a{c.Value}", isEsc ? [1, 1] : []);
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindEscapedPositionsOffsetData))]
    public void FindEscapedPositions_CharMaxSize(string input, int[] expectedOffsets)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        int maxSize = StringUtils.FindMaxEscapePositionSize(input.AsSpan());
        var list = new FastList<int>();
        fixed (byte* ptr = bytes)
        {
            StringUtils.FindEscapedPositions(list, ptr, bytes.Length, maxSize);
        }
        Assert.Equal(expectedOffsets, list.ToArray());
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindEscapedPositionsOffsetData))]
    public void FindEscapedPositions_ByteMaxSize(string input, int[] expectedOffsets)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        var list = new FastList<int>();
        fixed (byte* ptr = bytes)
        {
            int maxSize = StringUtils.FindMaxEscapePositionSize(ptr, bytes.Length);
            StringUtils.FindEscapedPositions(list, ptr, bytes.Length, maxSize);
        }
        Assert.Equal(expectedOffsets, list.ToArray());
    }

    // ── FindMaxEscapePositionSize ────────────────────────────────────────────
    //
    // All chars < 32, '"', and '\' are counted as escape chars.
    // formula: (escapeCount + 1) * 5
    //   Escape or Control → size = 10
    //   None              → size = 5

    public static TheoryData<string, int> FindMaxEscapePositionSizeData
    {
        get
        {
            var data = new TheoryData<string, int>();
            foreach (var c in Chars)
            {
                var size = c.Type == CharType.None ? 5 : 10;
                data.Add($"{c.Value}", size);
                data.Add($"{c.Value}a", size);
                data.Add($"a{c.Value}a", size);
                data.Add($"a{c.Value}", size);
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindMaxEscapePositionSizeData))]
    public void FindMaxEscapePositionSize_Bytes(string input, int expectedSize)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        fixed (byte* ptr = bytes)
        {
            Assert.Equal(expectedSize, StringUtils.FindMaxEscapePositionSize(ptr, bytes.Length));
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindMaxEscapePositionSizeData))]
    public void FindMaxEscapePositionSize_Chars(string input, int expectedSize) =>
        Assert.Equal(expectedSize, StringUtils.FindMaxEscapePositionSize(input.AsSpan()));

    // ── HasControlCharacters ────────────────────────────────────────────────

    public static TheoryData<string, bool> HasControlCharactersData
    {
        get
        {
            var data = new TheoryData<string, bool>();
            foreach (var c in Chars)
            {
                var isControl = c.Type == CharType.Control;
                data.Add($"{c.Value}", isControl);
                data.Add($"{c.Value}a", isControl);
                data.Add($"a{c.Value}a", isControl);
                data.Add($"a{c.Value}", isControl);
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(HasControlCharactersData))]
    public void HasControlCharacters_Byte(string input, bool expected) =>
        Assert.Equal(expected, StringUtils.HasControlCharacters(Encoding.UTF8.GetBytes(input).AsSpan()));

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(HasControlCharactersData))]
    public void HasControlCharacters_Char(string input, bool expected) =>
        Assert.Equal(expected, StringUtils.HasControlCharacters(input.AsSpan()));

    // ── FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility ──────
    //
    // formula: (escapeCount + 1) * 5 + controlCount * 5
    //   Escape → escapeCount++  → (1+1)*5 = 10, controlCount = 0
    //   Control     → controlCount++ → (0+1)*5 + 1*5 = 10, controlCount = 1
    //   None        →                → (0+1)*5 = 5,  controlCount = 0

    public static TheoryData<string, int, int> FindMaxEscapePositionAndControlCharSizeData
    {
        get
        {
            var data = new TheoryData<string, int, int>();
            foreach (var c in Chars)
            {
                var (size, controlCount) = c.Type switch
                {
                    CharType.Control => (10, 1),
                    CharType.Escape => (10, 0),
                    _ => (5, 0),
                };
                data.Add($"{c.Value}", size, controlCount);
                data.Add($"{c.Value}a", size, controlCount);
                data.Add($"a{c.Value}a", size, controlCount);
                data.Add($"a{c.Value}", size, controlCount);
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindMaxEscapePositionAndControlCharSizeData))]
    public void FindMaxEscapePositionAndControlCharSize_Bytes(string input, int expectedSize, int expectedControlCount)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        fixed (byte* ptr = bytes)
        {
            int result = StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(ptr, bytes.Length, out int controlCount);
            Assert.Equal(expectedSize, result);
            Assert.Equal(expectedControlCount, controlCount);
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindMaxEscapePositionAndControlCharSizeData))]
    public void FindMaxEscapePositionAndControlCharSize_Chars(string input, int expectedSize, int expectedControlCount)
    {
        int result = StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(input.AsSpan(), out int controlCount);
        Assert.Equal(expectedSize, result);
        Assert.Equal(expectedControlCount, controlCount);
    }

    // ── FindEscapedPositionsAndEscapeControlsForBackwardCompatibility ────────
    //
    //   Escape → offset recorded in list, length unchanged
    //   Control     → expanded in-place to \uXXXX (len += 5), nothing added to list
    //   None        → nothing

    public static TheoryData<int, string, int, int[], string> FindEscapedPositionsData
    {
        get
        {
            var data = new TheoryData<int, string, int, int[], string>();
            foreach (var c in Chars)
            {
                var expected = c.EscapeSequence ?? c.Value.ToString();
                data.Add(c.Value, $"{c.Value}", c.Type == CharType.Control ? 6 : 1, c.Type == CharType.Escape ? [0] : [], $"{expected}");
                data.Add(c.Value, $"{c.Value}a", (c.Type == CharType.Control ? 6 : 1) + 1, c.Type == CharType.Escape ? [0] : [], $"{expected}a");
                data.Add(c.Value, $"a{c.Value}a", 1 + (c.Type == CharType.Control ? 6 : 1) + 1, c.Type == CharType.Escape ? [1] : [], $"a{expected}a");
                data.Add(c.Value, $"a{c.Value}", 1 + (c.Type == CharType.Control ? 6 : 1), c.Type == CharType.Escape ? [1] : [], $"a{expected}");
                data.Add(c.Value, $"a{c.Value}a{c.Value}", 2 + 2 * (c.Type == CharType.Control ? 6 : 1), c.Type == CharType.Escape ? [1, 1] : [], $"a{expected}a{expected}");
            }
            return data;
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [MemberData(nameof(FindEscapedPositionsData))]
    public void FindEscapedPositionsAndEscapeControls(int intVal, string input, int expectedLen, int[] expectedOffsets, string expectedContent)
    {
        int maxSize = StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(input.AsSpan(), out _);
        var buf = new byte[input.Length + maxSize];
        Encoding.UTF8.GetBytes(input, buf);

        var list = new FastList<int>();
        int len = input.Length;

        fixed (byte* ptr = buf)
        {
            StringUtils.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(list, ptr, ref len, maxSize);

            Assert.Equal(expectedLen, len);
            Assert.Equal(expectedOffsets, list.ToArray());
            Assert.Equal(expectedContent, Encoding.UTF8.GetString(buf, 0, len));
        }
    }
}
