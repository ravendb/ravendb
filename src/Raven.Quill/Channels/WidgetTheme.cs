namespace Raven.Quill.Channels;

public enum WidgetAppearance
{
    Light,
    Dark,

    /// Follows the embedding visitor's `prefers-color-scheme`.
    System,
}

/// A named size rather than raw pixels, so one choice rounds every corner the widget draws.
/// The pixel values live in `RADIUS_SCALE` in `packages/widget/src/widget-theme.ts`.
public enum WidgetRadius
{
    None,
    Small,
    Medium,
    Large,
}

/// A named size scales the whole widget's type; `Custom` uses <see cref="WidgetTheme.CustomFontSizeRem"/>.
/// The rem values live in `FONT_SIZE_REM` in `packages/widget/src/widget-theme.ts`.
public enum WidgetFontSize
{
    Small,
    Medium,
    Large,
    Custom,
}

/// The logo's own rounding, independent of <see cref="WidgetRadius"/>: logos are usually avatars, so the
/// default is a full circle. The CSS values live in `LOGO_RADIUS_SCALE` in `packages/widget/src/widget-theme.ts`.
public enum WidgetLogoRadius
{
    None,
    Small,
    Medium,
    Large,
    Pill,
}

/// The colors an operator picks for one scheme. Everything else the widget paints is derived from these,
/// so light and dark each get their own trio without an operator ever filling in a swatch table.
public sealed record WidgetThemeColors(
    string ButtonColor,
    string MessageColor,
    string BackgroundColor);

/// The whole of a web widget's look and copy. Colors are per scheme (<see cref="Light"/> / <see cref="Dark"/>);
/// <see cref="Appearance"/> picks which scheme is the default, and an embedding page can override it per
/// visitor with a `?appearance=` query parameter or an `appearance` postMessage. Everything else is shared
/// between the schemes. <see cref="CustomCss"/> is the escape hatch for whatever the derived palette cannot
/// express; it is appended after the widget's own styles.
public sealed record WidgetTheme(
    WidgetAppearance Appearance,
    WidgetThemeColors Light,
    WidgetThemeColors Dark,
    WidgetRadius Radius,
    string FontFamily,
    WidgetFontSize FontSize,
    // Only read when FontSize is Custom; the widget clamps it to 0.625-1.5.
    double? CustomFontSizeRem,
    // A data:image/... URI, small enough to live in the document; the embed CSP allows `img-src data:`.
    string? Logo,
    WidgetLogoRadius LogoRadius,
    string HeaderTitle,
    string? HeaderSubtitle,
    bool ShowHeader,
    string? GreetingTitle,
    string? GreetingBody,
    string[] SuggestedPrompts,
    string InputPlaceholder,
    string? Disclaimer,
    string? CustomCss)
{
    /// Kept in step with `DEFAULT_THEME` in `packages/widget/src/widget-theme.ts`; the widget merges a
    /// server payload over its own defaults, so a drift here degrades gracefully rather than breaking.
    public static WidgetTheme Default { get; } = new(
        Appearance: WidgetAppearance.System,
        // The message colors are the button color mixed into the background (12% light, 24% dark), the mix
        // the widget's palette derivation used before the message color became its own option.
        Light: new WidgetThemeColors("#5b4bd6", MessageColor: "#ebe9fa", BackgroundColor: "#ffffff"),
        Dark: new WidgetThemeColors("#5b4bd6", MessageColor: "#201f45", BackgroundColor: "#0d1117"),
        Radius: WidgetRadius.Medium,
        FontFamily: WidgetFonts.SystemStack,
        FontSize: WidgetFontSize.Medium,
        CustomFontSizeRem: null,
        Logo: null,
        LogoRadius: WidgetLogoRadius.Pill,
        HeaderTitle: "AI Assistant",
        HeaderSubtitle: "Ask me anything",
        ShowHeader: true,
        GreetingTitle: "How can I help?",
        GreetingBody: "Ask a question and I'll do my best to answer it.",
        SuggestedPrompts: [],
        InputPlaceholder: "Ask a question...",
        Disclaimer: null,
        CustomCss: null);
}
