namespace Raven.AiAppliance.Contracts;

/// <summary>Result of minting an embed link.</summary>
/// <param name="Token">The opaque bearer token (a <c>chats/{guid}</c>-style id).
/// It is the credential in the iframe URL.</param>
/// <param name="Url">Absolute, paste-ready embed URL
/// (<c>{scheme}://{host}/embed/{token}</c>) for the customer's cross-origin
/// <c>&lt;iframe src&gt;</c>. Built from the mint request's host.</param>
/// <param name="ExpiresAt">UTC instant the link dies (TTL).</param>
/// <param name="MaxInvocations">The hard chat-turn cap applied to the link.</param>
public sealed record MintEmbedLinkResponse(
    string Token,
    string Url,
    DateTime ExpiresAt,
    int MaxInvocations);
