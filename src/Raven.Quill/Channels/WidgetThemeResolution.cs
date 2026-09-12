namespace Raven.Quill.Channels;

/// The single trust boundary for a stored theme. Everything downstream — the embed shell, the notice
/// pages, the widget's own config block — is handed the value this returns, so a document written outside
/// the PUT path is discarded here rather than being re-checked (differently, and too late) by each consumer.
internal static class WidgetThemeResolution
{
    internal static WidgetTheme ForDefaults(WidgetThemeDefaults? defaults) => Trusted(defaults?.Theme);

    /// A channel with no theme of its own follows the app default. The channel's display name fills in for
    /// an unset header title so the embed document still has a recognisable one.
    internal static WidgetTheme ForChannel(Channel channel, WidgetThemeDefaults? defaults)
    {
        var theme = Trusted(channel.Theme ?? defaults?.Theme);
        return string.IsNullOrWhiteSpace(theme.HeaderTitle) && string.IsNullOrWhiteSpace(channel.DisplayName) == false
            ? theme with { HeaderTitle = HeaderTitleFrom(channel.DisplayName) }
            : theme;
    }

    /// Display names are allowed to be far longer than a header title, and a substituted one still has to
    /// clear the theme's own bounds — otherwise the fill-in would invalidate the very theme it completes.
    private static string HeaderTitleFrom(string displayName)
    {
        var trimmed = displayName.Trim();
        return trimmed.Length <= WidgetThemeValidation.MaxHeaderTitleLength
            ? trimmed
            : trimmed[..WidgetThemeValidation.MaxHeaderTitleLength].TrimEnd();
    }

    /// Discarded whole rather than field by field, so nothing half-trusted reaches a stylesheet.
    private static WidgetTheme Trusted(WidgetTheme? theme) =>
        theme is not null && WidgetThemeValidation.TryValidate(theme, out _) ? theme : WidgetTheme.Default;
}
