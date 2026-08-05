using Raven.Quill.WhatsApp;

namespace Raven.Quill.Contracts;

// Qr is the raw linked-device payload; the dashboard renders it client-side and
// polls this endpoint, so the payload is always the current (rotating) one.
public sealed record WhatsAppPairingResponse(
    WhatsAppSessionState State,
    string? Qr,
    DateTime? QrExpiresAt,
    string? PhoneNumber,
    string? LastError);
