namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Public iFrame chat input posted by the embed widget to
/// <c>POST /embed/{widgetId}/chat</c>. No auth token in the 8-week demo — the
/// widgetId in the URL is the only credential (token hardening is a follow-up).
/// </summary>
/// <param name="Prompt">The end-user's message.</param>
/// <param name="ConversationId">Optional <c>chats/</c>-prefixed id to continue
/// a thread across turns; <c>null</c> starts a fresh conversation.</param>
public sealed record EmbedChatRequest(
    string Prompt,
    string? ConversationId = null);
