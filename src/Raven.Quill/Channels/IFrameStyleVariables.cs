namespace Raven.Quill.Channels;

/// <summary>
/// Catalog of CSS custom properties declared by the embed page's base stylesheet (see
/// <c>EmbedEndpoints.WidgetBaseCss</c>, whose <c>:root</c> block is generated from
/// <see cref="BuildRootBlock"/>), with one value per built-in preset. Keeping the catalog as
/// the single source means the shipped <c>:root</c> defaults can't drift from what the widget
/// actually renders, and the Light/Dark presets can't drift from each other structurally.
/// </summary>
internal static class IFrameStyleVariables
{
    private const string PrimaryColor = "#388ee9";

    internal static readonly IReadOnlyList<IFrameStyleVariable> All =
    [
        new("--ai-bg", Light: "#ffffff", Dark: "#0f1425"),
        new("--ai-fg", Light: "#0f172a", Dark: "#e5e9f5"),
        new("--ai-border-color", Light: "#e2e8f0", Dark: "#252d4a"),
        new("--ai-bubble-agent-bg", Light: "#f1f5f9", Dark: "#1b2340"),
        new("--ai-user-bg", PrimaryColor),
        new("--ai-user-fg", "#ffffff"),
        new("--ai-input-bg", Light: "#ffffff", Dark: "#161d36"),
        new("--ai-input-border-color", Light: "#cbd5e1", Dark: "#303a5e"),
        new("--ai-radius-bubble", "12px"),
        new("--ai-radius-control", "8px"),
        new("--ai-font-family", """system-ui, -apple-system, "Segoe UI", Roboto, sans-serif"""),
    ];

    /// <summary>Renders the catalog as the <c>:root { ... }</c> declaration block for the given
    /// preset, prepended to <c>EmbedEndpoints.WidgetBaseCssRules</c> to form the widget's base
    /// stylesheet. <see cref="IFrameStyle.Custom"/> renders the Light block — custom CSS layers
    /// over the light base, matching the styling editor's starter template.</summary>
    internal static string BuildRootBlock(IFrameStyle style) =>
        ":root {\n" + string.Join('\n', All.Select(variable => $"  {variable.Name}: {variable.ValueFor(style)};")) + "\n}";
}

/// <param name="Name">CSS custom property name, e.g. <c>--ai-user-bg</c>.</param>
/// <param name="Light">The value shipped in the Light preset's base stylesheet.</param>
/// <param name="Dark">The value shipped in the Dark preset's base stylesheet.</param>
internal sealed record IFrameStyleVariable(string Name, string Light, string Dark)
{
    /// <summary>A variable whose value is the same in both presets.</summary>
    internal IFrameStyleVariable(string name, string value) : this(name, value, value)
    {
    }

    internal string ValueFor(IFrameStyle style) => style == IFrameStyle.Dark ? Dark : Light;
}
