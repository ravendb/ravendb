namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Public iFrame chat input posted by the embed widget to
/// <c>POST /embed/{widgetId}/chat</c>.
/// </summary>
/// <param name="Prompt">The end-user's message.</param>
/// <param name="ConversationId">The id the widget received in the previous
/// turn's <c>done</c> frame, to continue the thread; <c>null</c> starts a
/// fresh conversation. The server mints this as a random <c>chats/{guid}</c>
/// on turn 1 — unguessable, so it is not enumerable (ayende's A2). It is
/// pinned to the <c>chats/</c> prefix server-side so it can't address an
/// unrelated document.</param>
public sealed record EmbedChatRequest(
    string Prompt,
    string? ConversationId = null);
