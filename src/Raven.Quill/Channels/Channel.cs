namespace Raven.Quill.Channels;

internal sealed class Channel
{
    internal const string IdPrefix = "channels/";

    public string? Id { get; set; }

    public ChannelType Type { get; set; }

    public string DisplayName { get; set; } = "";

    public string AgentId { get; set; } = "";

    public string[] AllowedOrigins { get; set; } = [];

    public bool Enabled { get; set; } = true;

    public DateTime CreatedAt { get; set; }

    public IFrameStyle? Style { get; set; }

    public string? CustomCss { get; set; }
}
