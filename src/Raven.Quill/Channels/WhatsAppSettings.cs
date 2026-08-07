namespace Raven.Quill.Channels;

internal sealed class WhatsAppSettings
{
    /// Bound per message from the sender's phone number instead of at provision time.
    internal const string WhatsAppUserIdentifierParameterName = "WhatsAppUserIdentifier";

    internal static bool IsAutoBoundParameter(string name) =>
        string.Equals(name, WhatsAppUserIdentifierParameterName, StringComparison.OrdinalIgnoreCase);

    /// Set once pairing completes; cleared when the phone unlinks the device.
    public string? PhoneNumber { get; set; }

    public DateTime? LinkedAt { get; set; }

    public Dictionary<string, string> Parameters { get; set; } = new();
}
