using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Contracts;

public sealed record ConversationDto(
    string Id,
    string AppId,
    string ChannelName,
    string AgentName,
    string AgentInitials,
    ConversationParam[] Params,
    AiConversationMessage[] LastExchange,
    AiConversationMessage[] Transcript,
    string State,
    DateTime LastActivityAt,
    DateTime? StartedAt,
    string? MaxDuration);

public sealed record ConversationParam(string Key, string Value);

public sealed record ConversationListResult(List<ConversationDto> Conversations, long TotalResults);
