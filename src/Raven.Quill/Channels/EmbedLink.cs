namespace Raven.Quill.Channels;

internal sealed class EmbedLink
{
    internal const string IdPrefix = "embed-links/";

    internal static bool IsWellFormedToken(string? token)
    {
        if (token is not { Length: 32 })
            return false;

        foreach (var c in token)
        {
            if (c is (< '0' or > '9') and (< 'a' or > 'f'))
                return false;
        }

        return true;
    }

    internal static string RedactToken(string? token) =>
        token is { Length: > 8 } ? token[..8] : (token ?? "");

    public string? Id { get; set; }

    public string WidgetId { get; set; } = "";

    public string AgentId { get; set; } = "";

    public Dictionary<string, string> Parameters { get; set; } = new();

    public DateTime ExpiresAt { get; set; }

    public int MaxInvocations { get; set; }

    public int InvocationCount { get; set; }

    public string? ConversationId { get; set; }

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
