namespace Raven.Quill.Channels;

internal readonly record struct ResolvedIFrameStyle(IFrameStyle Style, string? CustomCss);

internal static class IFrameStyleResolution
{
    internal static IFrameStyle? OwnStyle(Channel channel) =>
        channel.Style ?? (string.IsNullOrWhiteSpace(channel.CustomCss) ? null : IFrameStyle.Custom);

    internal static ResolvedIFrameStyle ForDefaults(IFrameStyleDefaults? defaults)
    {
        var style = defaults?.Style
                    ?? (string.IsNullOrWhiteSpace(defaults?.Css) ? IFrameStyle.Light : IFrameStyle.Custom);
        return new(style, style == IFrameStyle.Custom ? defaults?.Css : null);
    }

    internal static ResolvedIFrameStyle ForChannel(Channel channel, IFrameStyleDefaults? defaults)
    {
        var own = OwnStyle(channel);
        if (own is null)
            return ForDefaults(defaults);

        return new(own.Value, own == IFrameStyle.Custom ? channel.CustomCss : null);
    }
}
