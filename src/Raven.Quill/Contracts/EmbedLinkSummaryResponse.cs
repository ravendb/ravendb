using Raven.Quill.Agents;
using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record EmbedLinkSummaryResponse(
    string Token,
    string ChannelId,
    string AgentId,
    Dictionary<string, string> Parameters,
    DateTime CreatedAt,
    DateTime ExpiresAt,
    int MaxInvocations,
    int InvocationCount)
{
    internal static EmbedLinkSummaryResponse From(EmbedLink link) => new(
        link.ShortId,
        link.ChannelId,
        link.AgentId,
        link.Parameters.ToDictionary(
            parameter => parameter.Key,
            parameter => AgentParameterValue.ToDisplayText(AgentParameterValue.FromStoredText(parameter.Value))),
        link.CreatedAt,
        link.ExpiresAt,
        link.MaxInvocations,
        link.InvocationCount);
}
