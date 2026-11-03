namespace Raven.Quill.Channels;

internal sealed class SlackSettings
{
    internal const string AppTokenMissingError =
        "this channel predates Socket Mode; rotate its credentials and paste the app-level (xapp-) token to connect it";

    internal static string? SetupErrorOf(SlackSettings? settings) =>
        settings is { AppToken.Length: 0 } ? AppTokenMissingError : null;

    public string TeamId { get; set; } = "";

    public string TeamName { get; set; } = "";

    public string BotUserId { get; set; } = "";

    public string BotToken { get; set; } = "";

    public string AppToken { get; set; } = "";

    public DateTime ConnectedAt { get; set; }

    public Dictionary<string, ChannelParameterBinding> ParameterBindings { get; set; } = new();
}
