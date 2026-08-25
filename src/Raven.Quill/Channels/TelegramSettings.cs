namespace Raven.Quill.Channels;

internal sealed class TelegramSettings
{
    public string BotToken { get; set; } = "";

    public long BotId { get; set; }

    public string BotUsername { get; set; } = "";

    public Dictionary<string, ChannelParameterBinding> ParameterBindings { get; set; } = new();

    public TelegramChannelMessages? Messages { get; set; }
}
