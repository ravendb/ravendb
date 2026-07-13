namespace Raven.Quill.Channels;

/// <summary>
/// Catalog of CSS custom properties declared by the embed page's base stylesheet (see
/// <c>EmbedEndpoints.WidgetBaseCss</c>, whose <c>:root</c> block is generated from
/// <see cref="BuildRootBlock"/>). Keeping the catalog as the single source means the shipped
/// <c>:root</c> defaults can't drift from what the widget actually renders.
/// </summary>
internal static class IFrameStyleVariables
{
    internal static readonly IReadOnlyList<IFrameStyleVariable> All =
    [
        new("--ai-bg", "#ffffff"),
        new("--ai-fg", "#0f172a"),
        new("--ai-border-color", "#e2e8f0"),
        new("--ai-bubble-agent-bg", "#f1f5f9"),
        new("--ai-user-bg", "#2563eb"),
        new("--ai-user-fg", "#ffffff"),
        new("--ai-input-border-color", "#cbd5e1"),
        new("--ai-radius-bubble", "12px"),
        new("--ai-radius-control", "8px"),
        new("--ai-font-family", """system-ui, -apple-system, "Segoe UI", Roboto, sans-serif"""),
    ];

    /// <summary>Renders the catalog as the <c>:root { ... }</c> declaration block prepended to
    /// <c>EmbedEndpoints.WidgetBaseCssRules</c> to form the widget's base stylesheet.</summary>
    internal static string BuildRootBlock() =>
        ":root {\n" + string.Join('\n', All.Select(variable => $"  {variable.Name}: {variable.DefaultValue};")) + "\n}";
}

/// <param name="Name">CSS custom property name, e.g. <c>--ai-user-bg</c>.</param>
/// <param name="DefaultValue">The value shipped in the base stylesheet.</param>
internal sealed record IFrameStyleVariable(string Name, string DefaultValue);
