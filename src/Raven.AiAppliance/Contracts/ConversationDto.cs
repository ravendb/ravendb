namespace Raven.AiAppliance.Contracts;

/// <summary>
/// A conversation for the Conversations page — the prototype's <c>Conversation</c>,
/// shaped from a <c>@conversations</c> doc. <c>channelName</c> is empty (no channel
/// link on the doc yet — iframe attribution is a follow-up); <c>agentInitials</c>/
/// <c>agentColor</c> are derived from the agent id; <c>state</c> is derived from
/// <c>lastActivityAt</c> (active &lt;1h / idle &lt;24h / else closed).
/// </summary>
public sealed record ConversationDto(
    string Id,
    string AppId,
    string ChannelName,
    string AgentName,
    string AgentInitials,
    string AgentColor,
    ConversationParam[] Params,
    ConversationTurn[] LastExchange,
    ConversationTurn[]? Transcript,
    string State,
    DateTime LastActivityAt,
    DateTime? StartedAt,
    string? MaxDuration);

public sealed record ConversationParam(string Key, string Value);

public sealed record ConversationTurn(string Role, string Text, DateTime? At);
