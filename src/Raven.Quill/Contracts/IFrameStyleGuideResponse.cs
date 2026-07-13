namespace Raven.Quill.Contracts;

/// <summary>
/// The web-widget (iFrame) embed page's stylesheet building blocks, surfaced for the
/// dashboard styling editor.
/// </summary>
/// <param name="BaseCss">The widget's full base stylesheet — selectors and the Light preset's
/// <c>:root</c> variable declarations — verbatim what the live embed page injects before
/// operator CSS. Pre-fills a blank custom-CSS editor with a full starting template.</param>
/// <param name="LightThemeCss">The Light preset's <c>:root</c> variable block, injected into
/// the preview's custom-style slot to live-preview the preset.</param>
/// <param name="DarkThemeCss">The Dark preset's <c>:root</c> variable block, injected into
/// the preview's custom-style slot to live-preview the preset.</param>
public sealed record IFrameStyleGuideResponse(string BaseCss, string LightThemeCss, string DarkThemeCss);
