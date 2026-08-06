namespace Raven.Quill.WhatsApp;

// Serialized into the pairing contract (PascalCase names via the global enum
// converter) and parsed from the bridge's camelCase JSON by the client below.
public enum WhatsAppSessionState
{
    Starting,
    Pairing,
    Connected,
    Disconnected,
    LoggedOut,
}

internal sealed record WhatsAppSessionStatus(
    WhatsAppSessionState State,
    string? Qr,
    DateTime? QrExpiresAt,
    string? PairingCode,
    string? PhoneNumber,
    string? LastError);

/// The bridge could not be reached or answered outside its contract; endpoints map this to 502.
internal sealed class WhatsAppBridgeException(string message, Exception? inner = null)
    : Exception(message, inner);

/// The bridge refused a send because the session is not connected (bridge 409).
internal sealed class WhatsAppSendConflictException()
    : Exception("whatsapp session is not connected");

internal interface IWhatsAppBridgeClient
{
    /// A phone number links by pairing code instead of QR; null keeps the QR flow.
    Task StartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct);

    /// Returns null when the bridge does not know the session (bridge 404).
    Task<WhatsAppSessionStatus?> GetSessionStatusAsync(string database, string channelId, CancellationToken ct);

    Task RestartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct);

    Task SendTextAsync(string database, string channelId, string toJid, string text, CancellationToken ct);

    Task DeleteSessionAsync(string database, string channelId, CancellationToken ct);
}
