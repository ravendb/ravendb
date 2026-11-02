namespace Raven.Quill.Channels;

/// The app-level default theme, applied to every embedded chat channel that makes no choice of its own.
internal sealed class WidgetThemeDefaults
{
    internal const string DocumentId = "widget-theme-defaults/config";

    public string? Id { get; set; }

    public WidgetTheme? Theme { get; set; }

    public DateTime? UpdatedAt { get; set; }
}
