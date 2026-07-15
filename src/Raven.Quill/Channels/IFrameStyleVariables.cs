namespace Raven.Quill.Channels;

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

    internal static string BuildRootBlock(IFrameStyle style) =>
        ":root {\n" + string.Join('\n', All.Select(variable => $"  {variable.Name}: {variable.ValueFor(style)};")) + "\n}";
}

internal sealed record IFrameStyleVariable(string Name, string Light, string Dark)
{
    internal IFrameStyleVariable(string name, string value) : this(name, value, value)
    {
    }

    internal string ValueFor(IFrameStyle style) => style == IFrameStyle.Dark ? Dark : Light;
}
