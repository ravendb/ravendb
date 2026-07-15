using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record EmbedLinkSummaryResponse(
    string Token,
    string WidgetId,
    string AgentId,
    Dictionary<string, string> Parameters,
    DateTime CreatedAt,
    DateTime ExpiresAt,
    int MaxInvocations,
    int InvocationCount)
{
    internal static EmbedLinkSummaryResponse From(EmbedLink link) => new(
        StripPrefix(link.Id),
        link.WidgetId,
        link.AgentId,
        link.Parameters,
        link.CreatedAt,
        link.ExpiresAt,
        link.MaxInvocations,
        link.InvocationCount);

    private static string StripPrefix(string? id) =>
        id is not null && id.StartsWith(EmbedLink.IdPrefix, StringComparison.Ordinal)
            ? id[EmbedLink.IdPrefix.Length..]
            : id ?? "";
}
