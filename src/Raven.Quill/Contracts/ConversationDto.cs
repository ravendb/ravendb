namespace Raven.Quill.Contracts;

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

public sealed record ConversationListResult(List<ConversationDto> Conversations, long TotalResults);
