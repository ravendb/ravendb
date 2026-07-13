namespace Raven.Quill.Contracts;

/// <summary>Input for <c>POST /embed/{token}/chat</c>.</summary>
/// <param name="Prompt">The end-user's message. This is the ONLY thing the
/// public chat surface accepts: the conversation id and the agent parameters are
/// owned by the minted <see cref="Raven.Quill.Channels.EmbedLink"/>
/// (RavenDB-26775), so a visitor can neither resume another conversation nor
/// inject parameters (e.g. another customer's id).</param>
public sealed record EmbedChatRequest(string Prompt);
