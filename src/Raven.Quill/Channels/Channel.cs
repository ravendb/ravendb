namespace Raven.Quill.Channels;

internal sealed class Channel
{
    internal const string IdPrefix = "channels/";

    public string? Id { get; set; }

    /// The API-facing channel id: the document id without the collection prefix. Internal so it is
    /// never persisted into the document.
    internal string ShortId =>
        Id is not null && Id.StartsWith(IdPrefix, StringComparison.Ordinal)
            ? Id[IdPrefix.Length..]
            : Id ?? "";

    public ChannelType Type { get; set; }

    public string DisplayName { get; set; } = "";

    public string AgentId { get; set; } = "";

    public string[] AllowedOrigins { get; set; } = [];

    public bool Enabled { get; set; } = true;

    public DateTime CreatedAt { get; set; }

    /// Null means "follow the app-level default" (see <see cref="WidgetThemeResolution"/>). Web-widget only.
    public WidgetTheme? Theme { get; set; }

    public TelegramSettings? Telegram { get; set; }
}
