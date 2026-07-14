namespace Raven.Quill.Channels;

/// <summary>The effective styling of an embed surface after inheritance and legacy fallbacks:
/// a built-in preset with no CSS, or <see cref="IFrameStyle.Custom"/> with the operator's CSS.</summary>
internal readonly record struct ResolvedIFrameStyle(IFrameStyle Style, string? CustomCss);

/// <summary>
/// Resolves the effective <see cref="IFrameStyle"/> of a channel or the app defaults. Docs
/// written before the <c>Style</c> field existed carry only CSS — a doc with CSS but no style
/// is treated as <see cref="IFrameStyle.Custom"/> so pre-existing customizations keep
/// rendering unchanged.
/// </summary>
internal static class IFrameStyleResolution
{
    /// <summary>The channel's own style choice, or null when it follows the app default.</summary>
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
