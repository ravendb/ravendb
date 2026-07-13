namespace Raven.Quill.Contracts;

/// <summary>
/// Saves web-widget (iFrame) embed CSS — shared by the per-channel and the app-default PUTs.
/// A <c>null</c> or empty <paramref name="Css"/> clears the stored CSS: a channel then falls
/// back to the app default, and the app default falls back to the widget's base styles.
/// </summary>
public sealed record UpdateIFrameCustomizationRequest(string? Css);
