using Raven.Quill.WhatsApp;

namespace Raven.Quill.Contracts;

public sealed record WhatsAppChannelHealthResponse(
    string ChannelId,
    string? PhoneNumber,
    bool Enabled,
    WhatsAppSessionState? State,
    string? LastError);
