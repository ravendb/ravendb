namespace Raven.Quill.Channels;

internal sealed class WhatsAppSettings
{
    /// Set once pairing completes; cleared when the phone unlinks the device.
    public string? PhoneNumber { get; set; }

    public DateTime? LinkedAt { get; set; }

    public Dictionary<string, string> Parameters { get; set; } = new();
}
