using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

/// <summary>
/// The resolved app-level default embed style applied to web-widget (iFrame) channels that
/// make no choice of their own: <see cref="IFrameStyle.Light"/> when nothing was ever saved.
/// </summary>
/// <param name="Css">The default's CSS, set when <paramref name="Style"/> is
/// <see cref="IFrameStyle.Custom"/>.</param>
public sealed record IFrameDefaultCustomizationResponse(IFrameStyle Style, string? Css);
