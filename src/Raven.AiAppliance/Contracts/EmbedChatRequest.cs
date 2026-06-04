namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Public iFrame chat input posted by the embed widget to
/// <c>POST /embed/{widgetId}/chat</c>.
/// </summary>
/// <param name="Prompt">The end-user's message.</param>
/// <param name="ConversationToken">Opaque <c>cnv_</c> continuation token the
/// widget received in the first turn's <c>conversation</c> frame; <c>null</c>
/// starts a fresh conversation. A raw <c>chats/</c> conversation id is NEVER
/// accepted here — that was ayende's A2: server-allocated ids are sequential,
/// so a guessed id could continue (and read) another user's chat. The token
/// resolves to the hidden real id via a <c>conversation-bindings/</c> doc.</param>
public sealed record EmbedChatRequest(
    string Prompt,
    string? ConversationToken = null);
