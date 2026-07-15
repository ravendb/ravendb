namespace Raven.Quill.Channels;

internal static class IFrameCss
{
    internal const int MaxLength = 100_000;

    private const string StyleClose = "</style";

    internal static bool TryValidate(string? css, out string? error)
    {
        error = null;
        if (string.IsNullOrEmpty(css))
            return true;

        if (css.Length > MaxLength)
        {
            error = $"css exceeds {MaxLength} chars";
            return false;
        }

        if (css.Contains(StyleClose, StringComparison.OrdinalIgnoreCase))
        {
            error = "css must not contain a '</style>' sequence";
            return false;
        }

        return true;
    }

    internal static string Sanitize(string? css) =>
        string.IsNullOrEmpty(css) ? "" : css.Replace(StyleClose, @"<\/style", StringComparison.OrdinalIgnoreCase);
}
