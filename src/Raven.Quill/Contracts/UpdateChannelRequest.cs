namespace Raven.Quill.Contracts;

public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled);
