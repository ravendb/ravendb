using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

/// <summary>
/// Saves a web-widget (iFrame) embed style — shared by the per-channel and the app-default
/// PUTs. <paramref name="Css"/> applies only when <paramref name="Style"/> is
/// <see cref="IFrameStyle.Custom"/> (then it is required). A null <paramref name="Style"/>
/// clears a channel's choice so it follows the app default; on the app-default PUT it resets
/// to <see cref="IFrameStyle.Light"/>.
/// </summary>
public sealed record UpdateIFrameCustomizationRequest(IFrameStyle? Style, string? Css);
