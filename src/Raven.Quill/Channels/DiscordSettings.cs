namespace Raven.Quill.Channels;

internal sealed class DiscordSettings
{
    public string ApplicationId { get; set; } = "";

    public string BotUserId { get; set; } = "";

    public string BotUsername { get; set; } = "";

    public string BotToken { get; set; } = "";

    public DateTime ConnectedAt { get; set; }

    public Dictionary<string, ChannelParameterBinding> ParameterBindings { get; set; } = new();
}
