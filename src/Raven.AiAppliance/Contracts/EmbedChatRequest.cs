namespace Raven.AiAppliance.Contracts;

/// <summary>Input for <c>POST /embed/{widgetId}/chat</c>.</summary>
/// <param name="Prompt">The end-user's message.</param>
/// <param name="ConversationId">The id echoed from the previous <c>done</c>
/// frame to continue the thread; <c>null</c> starts fresh. Server-minted as a
/// random <c>chats/{guid}</c> (unguessable, not enumerable) and pinned to the
/// <c>chats/</c> prefix.</param>
public sealed record EmbedChatRequest(
    string Prompt,
    string? ConversationId = null);
