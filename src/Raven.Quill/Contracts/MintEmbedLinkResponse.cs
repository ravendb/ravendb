namespace Raven.Quill.Contracts;

public sealed record MintEmbedLinkResponse(
    string Token,
    string Url,
    DateTime ExpiresAt,
    int MaxInvocations);
