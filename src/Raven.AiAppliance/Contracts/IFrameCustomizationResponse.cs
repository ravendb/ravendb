namespace Raven.AiAppliance.Contracts;

/// <summary>
/// A web-widget (iFrame) channel's embed-styling state for the dashboard editor.
/// </summary>
/// <param name="Css">The channel's own CSS. <c>null</c> means it has no override and renders
/// with the app default (or just the widget base styles).</param>
/// <param name="DefaultCss">The app-level default CSS, surfaced so the editor can preview the
/// effective styling when <paramref name="Css"/> is empty and offer a "reset to default" affordance.</param>
public sealed record IFrameCustomizationResponse(string? Css, string? DefaultCss);
