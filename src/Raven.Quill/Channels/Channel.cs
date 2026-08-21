namespace Raven.Quill.Channels;

internal sealed class Channel
{
    internal const string IdPrefix = "channels/";

    public string? Id { get; set; }

    internal string ShortId => ShortIdFor(Id ?? "");

    internal static string ShortIdFor(string id) =>
        id.StartsWith(IdPrefix, StringComparison.Ordinal) ? id[IdPrefix.Length..] : id;

    public ChannelType Type { get; set; }

    public string DisplayName { get; set; } = "";

    public string AgentId { get; set; } = "";

    public string[] AllowedOrigins { get; set; } = [];

    public bool Enabled { get; set; } = true;

    public DateTime CreatedAt { get; set; }

    /// Null means "follow the app-level default" (see <see cref="WidgetThemeResolution"/>). Web-widget only.
    public WidgetTheme? Theme { get; set; }

    public TelegramSettings? Telegram { get; set; }

    public SlackSettings? Slack { get; set; }
}
