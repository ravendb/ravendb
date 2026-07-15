namespace Raven.Quill.Channels;

// per-app (slug,type,agentId) uniqueness doc; the atomic guard serializes writers
internal sealed class ChannelBinding
{
    public string? Id { get; set; }

    public string WidgetId { get; set; } = "";

    public DateTime CreatedAt { get; set; }
}
