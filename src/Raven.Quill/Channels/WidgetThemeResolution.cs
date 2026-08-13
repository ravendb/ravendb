namespace Raven.Quill.Channels;

internal static class WidgetThemeResolution
{
    internal static WidgetTheme ForDefaults(WidgetThemeDefaults? defaults) =>
        defaults?.Theme ?? WidgetTheme.Default;

    /// A channel with no theme of its own follows the app default. The channel's display name fills in for
    /// an unset header title so a freshly created widget still shows something recognisable.
    internal static WidgetTheme ForChannel(Channel channel, WidgetThemeDefaults? defaults)
    {
        var theme = channel.Theme ?? ForDefaults(defaults);
        return string.IsNullOrWhiteSpace(theme.HeaderTitle) && string.IsNullOrWhiteSpace(channel.DisplayName) == false
            ? theme with { HeaderTitle = channel.DisplayName }
            : theme;
    }
}
