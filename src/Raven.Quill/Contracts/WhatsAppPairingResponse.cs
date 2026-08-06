using Raven.Quill.WhatsApp;

namespace Raven.Quill.Contracts;

// Qr is the raw linked-device payload; the dashboard renders it client-side and
// polls this endpoint, so the payload is always the current (rotating) one.
// PairingCode is set instead of Qr when linking by phone number.
public sealed record WhatsAppPairingResponse(
    WhatsAppSessionState State,
    string? Qr,
    DateTime? QrExpiresAt,
    string? PairingCode,
    string? PhoneNumber,
    string? LastError);

/// An empty body (or null number) restarts the QR flow; a number switches to a pairing code.
public sealed record WhatsAppPairingRestartRequest(string? PhoneNumber = null);
