namespace Raven.Quill.Channels;

public enum WidgetAppearance
{
    Light,
    Dark,

    /// Follows the embedding visitor's `prefers-color-scheme`.
    System,
}

public enum WidgetDensity
{
    Comfortable,
    Compact,
}

/// The whole of a web widget's look and copy. Every colour beyond <see cref="AccentColor"/> is derived
/// in the widget from the accent plus the appearance, so an operator never fills in a swatch table.
public sealed record WidgetTheme(
    WidgetAppearance Appearance,
    string AccentColor,
    int Radius,
    string FontFamily,
    WidgetDensity Density,
    string HeaderTitle,
    string? HeaderSubtitle,
    string? AvatarInitials,
    bool ShowHeader,
    string? GreetingTitle,
    string? GreetingBody,
    string[] SuggestedPrompts,
    string InputPlaceholder,
    string? Disclaimer)
{
    /// Kept in step with `DEFAULT_THEME` in `packages/widget/src/widget-theme.ts`; the widget merges a
    /// server payload over its own defaults, so a drift here degrades gracefully rather than breaking.
    public static WidgetTheme Default { get; } = new(
        Appearance: WidgetAppearance.System,
        AccentColor: "#5b4bd6",
        Radius: 12,
        FontFamily: WidgetFonts.SystemStack,
        Density: WidgetDensity.Comfortable,
        HeaderTitle: "AI Assistant",
        HeaderSubtitle: "Ask me anything",
        AvatarInitials: null,
        ShowHeader: true,
        GreetingTitle: "How can I help?",
        GreetingBody: "Ask a question and I'll do my best to answer it.",
        SuggestedPrompts: [],
        InputPlaceholder: "Ask a question...",
        Disclaimer: null);
}
