using System.Buffers;
using System.Text;

namespace Raven.Quill.Channels;

internal static class MessageSplitter
{
    internal static IReadOnlyList<string> Split(string text, int limit)
    {
        if (text.Length <= limit)
            return [text];

        var parts = new List<string>();
        var remaining = text.AsSpan();

        while (remaining.Length > limit)
        {
            var cut = CutPoint(remaining, limit);

            var part = remaining[..cut].TrimEnd();
            if (part.Length > 0)
                parts.Add(part.ToString());

            remaining = remaining[cut..].TrimStart();
        }

        if (remaining.Length > 0)
            parts.Add(remaining.ToString());

        return parts;
    }

    internal static int CutPoint(ReadOnlySpan<char> text, int limit)
    {
        if (text.Length <= limit)
            return text.Length;

        var cut = LastSentenceBoundary(text[..limit]);
        if (cut > 0)
            return cut;

        cut = limit;
        if (Rune.DecodeLastFromUtf16(text[..cut], out _, out _) != OperationStatus.Done)
            cut--;

        return Math.Max(1, cut);
    }

    private static int LastSentenceBoundary(ReadOnlySpan<char> window)
    {
        for (var i = window.Length - 1; i >= 0; i--)
        {
            if (window[i] is '.' or '!' or '?' or '\n')
                return i + 1;
        }

        return -1;
    }
}
