namespace Raven.AiAppliance.Contracts;

/// <summary>
/// The web-widget (iFrame) embed page's base CSS. Surfaced so the dashboard styling editor can
/// pre-fill a blank customization with a full starting template and restore it on "reset to default".
/// </summary>
/// <param name="BaseCss">The widget's full base stylesheet — selectors and the <c>:root</c>
/// variable declarations — verbatim what the live embed page injects before operator CSS.</param>
public sealed record IFrameStyleGuideResponse(string BaseCss);
