using Raven.Quill.WhatsApp;

namespace Raven.Quill.Contracts;

public sealed record WhatsAppPairingResponse(
    WhatsAppSessionState State,
    string? Qr,
    DateTime? QrExpiresAt,
    string? PairingCode,
    string? PhoneNumber,
    string? LastError);

public sealed record WhatsAppPairingRestartRequest(string? PhoneNumber = null);
