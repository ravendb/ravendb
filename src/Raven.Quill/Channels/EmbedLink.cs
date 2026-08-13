namespace Raven.Quill.Channels;

internal sealed class EmbedLink
{
    // token = Guid "N" (32 hex); 122 random bits, stored raw (app DB encrypted at rest)
    internal const string IdPrefix = "embed-links/";

    // validate shape first so a crafted token can't probe other docs
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

    // log first 8 chars only; the token is a bearer secret
    internal static string RedactToken(string? token) =>
        token is { Length: > 8 } ? token[..8] : (token ?? "");

    public string? Id { get; set; }

    internal string ShortId =>
        Id is not null && Id.StartsWith(IdPrefix, StringComparison.Ordinal)
            ? Id[IdPrefix.Length..]
            : Id ?? "";

    public string ChannelId { get; set; } = "";

    public string AgentId { get; set; } = "";

    public Dictionary<string, string> Parameters { get; set; } = new();

    public DateTime ExpiresAt { get; set; }

    public int MaxInvocations { get; set; }

    // OCC-incremented before each turn so the cap holds under concurrent turns
    public int InvocationCount { get; set; }

    public string? ConversationId { get; set; }

    public bool Revoked { get; set; }

    public DateTime CreatedAt { get; set; }
}
