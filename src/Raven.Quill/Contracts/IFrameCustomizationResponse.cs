using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

/// <summary>
/// A web-widget (iFrame) channel's embed-styling state for the dashboard editor.
/// </summary>
/// <param name="Style">The channel's own style choice. <c>null</c> means it follows the app
/// default described by <paramref name="DefaultStyle"/>/<paramref name="DefaultCss"/>.</param>
/// <param name="Css">The channel's own CSS, set when <paramref name="Style"/> is
/// <see cref="IFrameStyle.Custom"/>.</param>
/// <param name="DefaultStyle">The resolved app-level default style, surfaced so the editor
/// can describe and preview what "follow the app default" renders as.</param>
/// <param name="DefaultCss">The app-level default CSS, set when
/// <paramref name="DefaultStyle"/> is <see cref="IFrameStyle.Custom"/>.</param>
public sealed record IFrameCustomizationResponse(
    IFrameStyle? Style,
    string? Css,
    IFrameStyle DefaultStyle,
    string? DefaultCss);
