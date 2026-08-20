namespace Raven.Quill.WhatsApp;

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

internal sealed class WhatsAppBridgeException(string message, Exception? inner = null)
    : Exception(message, inner);

internal sealed class WhatsAppSendConflictException()
    : Exception("whatsapp session is not connected");

internal interface IWhatsAppBridgeClient
{
    Task StartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct);

    Task<WhatsAppSessionStatus?> GetSessionStatusAsync(string database, string channelId, CancellationToken ct);

    Task RestartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct);

    Task SendTextAsync(string database, string channelId, string toJid, string text, CancellationToken ct);

    Task DeleteSessionAsync(string database, string channelId, CancellationToken ct);
}
