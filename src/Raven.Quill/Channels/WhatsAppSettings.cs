namespace Raven.Quill.Channels;

internal sealed class WhatsAppSettings
{
    public string? PhoneNumber { get; set; }

    public DateTime? LinkedAt { get; set; }

    public Dictionary<string, TelegramParameterBinding> ParameterBindings { get; set; } = new();
}
