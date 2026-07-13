namespace Raven.Quill.Contracts;

/// <summary>
/// The app-level default embed CSS applied to web-widget (iFrame) channels that define no CSS
/// of their own. <c>null</c> means no default is set.
/// </summary>
public sealed record IFrameDefaultCustomizationResponse(string? Css);
