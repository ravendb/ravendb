using Raven.Quill.WhatsApp;

namespace Raven.Quill.Contracts;

// State is null when the bridge has no session for the channel (or is unreachable);
// the dashboard renders that as "not linked" rather than failing the whole list.
public sealed record WhatsAppChannelHealthResponse(
    string ChannelId,
    string? PhoneNumber,
    bool Enabled,
    WhatsAppSessionState? State,
    string? LastError);
