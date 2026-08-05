namespace Raven.Quill.Telegram;

internal static class TelegramMessageSplitter
{
    internal const int TelegramMessageLimit = 4096;

    internal static IReadOnlyList<string> Split(string text, int limit = TelegramMessageLimit)
    {
        if (text.Length <= limit)
            return [text];

        var parts = new List<string>();
        var remaining = text.AsSpan();

        while (remaining.Length > limit)
        {
            var cut = LastSentenceBoundary(remaining[..limit]);
            if (cut <= 0)
                cut = limit;

            var part = remaining[..cut].TrimEnd();
            if (part.Length > 0)
                parts.Add(part.ToString());

            remaining = remaining[cut..].TrimStart();
        }

        if (remaining.Length > 0)
            parts.Add(remaining.ToString());

        return parts;
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
