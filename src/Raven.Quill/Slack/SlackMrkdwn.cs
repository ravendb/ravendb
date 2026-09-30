using System.Text;
using System.Text.RegularExpressions;

namespace Raven.Quill.Slack;

internal static partial class SlackMrkdwn
{
    private const char Mask = '\uE000';

    internal const int MaxEscapeExpansion = 5;

    internal static string Escape(string text) =>
        string.IsNullOrEmpty(text)
            ? text
            : text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");

    internal static string Convert(string markdown)
    {
        if (string.IsNullOrEmpty(markdown))
            return markdown;

        markdown = Escape(markdown);

        var result = new StringBuilder(markdown.Length + 16);
        var inFence = false;
        var lines = markdown.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.TrimStart().StartsWith("```", StringComparison.Ordinal))
            {
                result.Append(inFence ? line : StripFenceLanguage(line));
                inFence = !inFence;
            }
            else
            {
                result.Append(inFence ? line : ConvertLine(line));
            }

            if (i < lines.Length - 1)
                result.Append('\n');
        }

        return result.ToString();
    }

    private static string StripFenceLanguage(string line) =>
        line[..(line.IndexOf("```", StringComparison.Ordinal) + 3)];

    private static string ConvertLine(string line)
    {
        if (line.Length == 0)
            return line;

        var spans = new List<string>();
        var masked = InlineCode().Replace(line, match =>
        {
            spans.Add(match.Value);
            return $"{Mask}{spans.Count - 1}{Mask}";
        });

        masked = Link().Replace(masked, "<$2|$1>");
        masked = BoldStars().Replace(masked, "*$1*");
        masked = BoldUnderscores().Replace(masked, "*$1*");
        masked = Strike().Replace(masked, "~$1~");
        masked = Heading().Replace(masked, match =>
        {
            var content = match.Groups[1].Value.Trim();
            return content.Length >= 2 && content.StartsWith('*') && content.EndsWith('*')
                ? content
                : $"*{content}*";
        });

        for (var i = 0; i < spans.Count; i++)
            masked = masked.Replace($"{Mask}{i}{Mask}", spans[i]);

        return masked;
    }

    [GeneratedRegex(@"`[^`]+`")]
    private static partial Regex InlineCode();

    [GeneratedRegex(@"\[([^\]]+)\]\((https?://[^)\s]+)\)")]
    private static partial Regex Link();

    [GeneratedRegex(@"\*\*(.+?)\*\*")]
    private static partial Regex BoldStars();

    [GeneratedRegex(@"__(.+?)__")]
    private static partial Regex BoldUnderscores();

    [GeneratedRegex(@"~~(.+?)~~")]
    private static partial Regex Strike();

    [GeneratedRegex(@"^\s{0,3}#{1,6}\s+(.+)$")]
    private static partial Regex Heading();
}
