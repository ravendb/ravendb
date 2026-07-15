namespace Raven.Quill.Channels;

internal sealed class LinkIndex
{
    internal const string IdPrefix = "link-index/";

    public string? Id { get; set; }

    public string Slug { get; set; } = "";
}
