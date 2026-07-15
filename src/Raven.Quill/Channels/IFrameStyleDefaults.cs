namespace Raven.Quill.Channels;

internal sealed class IFrameStyleDefaults
{
    internal const string DocumentId = "iframe-style-defaults/config";

    public string? Id { get; set; }

    public IFrameStyle? Style { get; set; }

    public string? Css { get; set; }

    public DateTime? UpdatedAt { get; set; }
}
