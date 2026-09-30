namespace Raven.Quill.Channels;

/// A font stack the operator can pick, surfaced with the theme so the dashboard renders the choices from
/// the server's list instead of a copy that drifts.
public sealed record WidgetFontOption(string Label, string Stack);

public static class WidgetFonts
{
    public const string SystemStack = """system-ui, -apple-system, "Segoe UI", Roboto, sans-serif""";

    /// Every stack resolves against fonts already on the visitor's device: the widget bundles no web font,
    /// which is what keeps `font-src` unused and the payload budget intact.
    public static IReadOnlyList<WidgetFontOption> Curated { get; } =
    [
        new("System", SystemStack),
        new("Grotesque sans", """"Helvetica Neue", Helvetica, Arial, sans-serif""""),
        new("Geometric sans", """Verdana, "DejaVu Sans", Tahoma, sans-serif"""),
        new("Serif", """Georgia, "Times New Roman", Times, serif"""),
        new("Transitional serif", """Charter, "Iowan Old Style", Palatino, serif"""),
        new("Monospace", """ui-monospace, "SF Mono", "Cascadia Mono", Menlo, Consolas, monospace"""),
    ];

    public static bool IsCurated(string stack) =>
        Curated.Any(option => string.Equals(option.Stack, stack, StringComparison.Ordinal));
}
