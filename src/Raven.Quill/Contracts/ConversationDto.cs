namespace Raven.Quill.Contracts;

/// <summary>
/// A conversation for the Conversations page, shaped from a <c>@conversations</c> doc.
/// <c>channelName</c> is resolved for iframe conversations via their embed link, and is
/// empty for conversations opened outside a minted embed link (e.g. the wizard test chat);
/// <c>agentInitials</c> is derived from the agent id; <c>state</c> is derived from
/// <c>lastActivityAt</c> (active &lt;1h / idle &lt;24h / else closed).
/// </summary>
public sealed record ConversationDto(
    string Id,
    string AppId,
    string ChannelName,
    string AgentName,
    string AgentInitials,
    ConversationParam[] Params,
    ConversationTurn[] LastExchange,
    ConversationTurn[]? Transcript,
    string State,
    DateTime LastActivityAt,
    DateTime? StartedAt,
    string? MaxDuration);

public sealed record ConversationParam(string Key, string Value);

public sealed record ConversationTurn(string Role, string Text, DateTime? At);
