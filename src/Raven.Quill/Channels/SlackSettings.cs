namespace Raven.Quill.Channels;

internal sealed class SlackSettings
{
    public string TeamId { get; set; } = "";

    public string TeamName { get; set; } = "";

    public string BotUserId { get; set; } = "";

    public string BotToken { get; set; } = "";

    public string SigningSecret { get; set; } = "";

    public string WebhookToken { get; set; } = "";

    public DateTime ConnectedAt { get; set; }

    public Dictionary<string, ChannelParameterBinding> ParameterBindings { get; set; } = new();
}
