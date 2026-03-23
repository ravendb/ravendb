using System;
using System.Globalization;
using System.Text;

namespace Sparrow.Json;

#if DEBUG
public static class DebugMemoryHelper
{
    private const int BytesPerLine = 16;

    public static unsafe string GetDebugView(byte* ptr, int size)
    {
        if (ptr == null)
            throw new ArgumentNullException(nameof(ptr));
        if (size < 0)
            throw new ArgumentOutOfRangeException(nameof(size));

        var sb = new StringBuilder();
        AppendHeader(sb);

        byte* start;
        byte* end;

        if (size == 0)
        {
            start = ptr;
            end = ptr;
        }
        else
        {
            start = ptr;
            end = ptr + size - 1;
        }

        byte* alignedStart = AlignPointer(ptr, size, out var totalLength);

        for (int offset = 0; offset < totalLength; offset += BytesPerLine)
        {
            byte* linePtr = alignedStart + offset;
            AppendLine(sb, linePtr, start, end);
        }

        return sb.ToString();
    }

    public static unsafe string GetDebugView(string hexPtr, int size)
    {
        if (string.IsNullOrWhiteSpace(hexPtr))
            throw new ArgumentNullException(nameof(hexPtr));

        ulong address = ulong.Parse(hexPtr, NumberStyles.HexNumber);
        byte* ptr = (byte*)address;

        return GetDebugView(ptr, size);
    }

    private static void AppendHeader(StringBuilder sb)
    {
        sb.Append(' ', 37);
        sb.AppendLine("0123456789abcdef");
    }

    private static unsafe byte* AlignPointer(byte* ptr, int size, out int totalLength)
    {
        // Compute extra bytes so that we start on a BytesPerLine boundary.
        int extra = (int)((long)ptr % BytesPerLine) + BytesPerLine;
        byte* aligned = ptr - extra;

        // Cover the requested size plus padding before and one extra line after.
        totalLength = size + extra + BytesPerLine;
        return aligned;
    }

    private static unsafe void AppendLine(StringBuilder sb, byte* linePtr, byte* start, byte* end)
    {
        AppendOffset(sb, linePtr);
        AppendHexRange(sb, linePtr, start, end, 0, 8);   // left hex (0–7)
        AppendAscii(sb, linePtr, start, end);
        sb.Append(' ');
        AppendHexRange(sb, linePtr, start, end, 8, BytesPerLine); // right hex (8–15)
        sb.AppendLine();
    }

    private static unsafe void AppendOffset(StringBuilder sb, byte* linePtr)
    {
        sb.Append($"{(long)linePtr:X8}: ");
    }

    private static unsafe void AppendHexRange(StringBuilder sb, byte* linePtr, byte* start, byte* end, int from, int to)
    {
        for (int i = from; i < to; i++)
        {
            byte* current = linePtr + i;
            AppendHexByteWithMarkers(sb, current, start, end);
        }
    }

    private static unsafe void AppendHexByteWithMarkers(StringBuilder sb, byte* current, byte* start, byte* end)
    {
        if(current == start)
            sb[sb.Length - 1] = '*';
        
        sb.Append($"{current[0]:X2}");
        sb.Append(current == end?'*':' ');
    }

    private static unsafe void AppendAscii(StringBuilder sb, byte* linePtr, byte* start, byte* end)
    {
        for (int j = 0; j < BytesPerLine; j++)
        {
            byte* current = linePtr + j;
            byte b = current[0];
            sb.Append(IsPrintableAscii(b) ? (char)b : '.');
        }
    }

    private static bool IsPrintableAscii(byte b) => b is >= 32 and <= 126;
}
#endif
