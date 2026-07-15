namespace Raven.Quill.Channels;

internal sealed class ChannelBinding
{
    public string? Id { get; set; }

    public string WidgetId { get; set; } = "";

    public DateTime CreatedAt { get; set; }
}
